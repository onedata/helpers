#include "asioExecutor.h"
#include "buffering/bufferAgent.h"
#include "helpers/storageHelper.h"
#include "scheduler.h"

#include <asio/io_service.hpp>
#include <asio/ts/executor.hpp>
#include <folly/FBString.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBufQueue.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>

class FileHandle : public one::helpers::FileHandle {
public:
    FileHandle(const folly::fbstring &fileId, folly::Executor &executor,
        std::atomic<bool> &wasSimultaneous)
        : one::helpers::FileHandle{fileId}
        , m_executor{executor}
        , m_wasSimultaneous{wasSimultaneous}
    {
    }

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override
    {
        return folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
    }

    folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf) override
    {
        return folly::via(&m_executor, [ this, size = buf.chainLength() ] {
            if (++m_simultaneous > 1)
                m_wasSimultaneous = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            --m_simultaneous;
            return size;
        });
    }

    const one::helpers::Timeout &timeout() override
    {
        return one::helpers::ASYNC_OPS_TIMEOUT;
    }

private:
    folly::Executor &m_executor;
    std::atomic<bool> &m_wasSimultaneous;
    std::atomic<size_t> m_simultaneous{0};
};

class StorageHelper : public one::helpers::StorageHelper {
public:
    StorageHelper(folly::Executor &executor, std::atomic<bool> &wasSimultaneous)
        : m_executor{executor}
        , m_wasSimultaneous{wasSimultaneous}
    {
    }

    folly::Future<one::helpers::FileHandlePtr> open(
        const folly::fbstring &fileId, const int /*flags*/,
        const one::helpers::Params & /*openParams*/) override
    {
        return folly::makeFuture(static_cast<one::helpers::FileHandlePtr>(
            std::make_shared<FileHandle>(
                fileId, m_executor, m_wasSimultaneous)));
    }

    const one::helpers::Timeout &timeout() override
    {
        return one::helpers::ASYNC_OPS_TIMEOUT;
    }

private:
    folly::Executor &m_executor;
    std::atomic<bool> &m_wasSimultaneous;
};

struct BufferAgentsMemoryLimitGuardTest : public ::testing::Test {
protected:
    BufferAgentsMemoryLimitGuardTest()
    {
        limits.readBufferMaxSize = 10'000'000;
        limits.writeBufferMaxSize = 5'000'000;
        limits.readBuffersTotalSize = 100'000'000;
        limits.writeBuffersTotalSize = 50'000'000;

        limitGuard = std::make_shared<
            one::helpers::buffering::BufferAgentsMemoryLimitGuard>(limits);
    }

    one::helpers::buffering::BufferLimits limits = {};
    std::shared_ptr<one::helpers::buffering::BufferAgentsMemoryLimitGuard>
        limitGuard;
};

TEST_F(BufferAgentsMemoryLimitGuardTest, shouldReserveMemoryForBufferedHandles)
{
    const auto readSize = limits.readBufferMaxSize;
    const auto writeSize = limits.writeBufferMaxSize;

    for (int i = 0; i < 10; i++)
        ASSERT_TRUE(limitGuard->reserveBuffers(readSize, writeSize));

    ASSERT_FALSE(limitGuard->reserveBuffers(readSize, writeSize));

    ASSERT_TRUE(limitGuard->releaseBuffers(readSize, writeSize));

    ASSERT_TRUE(limitGuard->reserveBuffers(readSize, writeSize));

    ASSERT_FALSE(limitGuard->reserveBuffers(readSize, writeSize));
}

struct BufferHelperTest : public ::testing::Test {
protected:
    BufferHelperTest()
    {
        for (int i = 0; i < 100; ++i)
            workers.emplace_back(std::thread{[&] { service.run(); }});

        one::helpers::buffering::BufferLimits limits;
        limits.readBufferMaxSize = 10 * 1024 * 1024;
        limits.writeBufferMaxSize = 50 * 1024 * 1024;
        limits.readBuffersTotalSize = 5 * limits.readBufferMaxSize;
        limits.writeBuffersTotalSize = 5 * limits.writeBufferMaxSize;

        auto memoryLimitGuard = std::make_shared<
            one::helpers::buffering::BufferAgentsMemoryLimitGuard>(limits);

        auto wrappedHelper =
            std::make_shared<StorageHelper>(executor, wasSimultaneous);
        helper = std::make_shared<one::helpers::buffering::BufferAgent>(
            limits, wrappedHelper, scheduler, std::move(memoryLimitGuard));
    }

    ~BufferHelperTest()
    {
        service.stop();
        for (auto &worker : workers)
            worker.join();
    }

    std::atomic<bool> wasSimultaneous{false};
    one::helpers::StorageHelperPtr helper;

private:
    asio::io_service service{100};
    asio::executor_work_guard<asio::io_service::executor_type> idleWork{
        asio::make_work_guard(service)};
    folly::fbvector<std::thread> workers;
    one::Scheduler scheduler{1};
    one::AsioExecutor executor{service};
};

TEST_F(BufferHelperTest, shouldNotWriteSimultaneously)
{
    auto handle = helper->open("fileId", 0, {}).get();

    ASSERT_TRUE(
        std::dynamic_pointer_cast<one::helpers::buffering::BufferedFileHandle>(
            handle));

    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 1000; ++j) {
            folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
            buf.allocate(1024 * 1024);
            handle->write(j * 1024 * 1024, std::move(buf));
        }
    }
    handle->release().get();
    ASSERT_FALSE(wasSimultaneous.load());
}

TEST_F(BufferHelperTest, shouldReleaseMemoryGuardReserves)
{
    std::vector<one::helpers::FileHandlePtr> handles;

    for (int i = 0; i < 5; i++) {
        auto handle = helper->open("fileId" + std::to_string(i), 0, {}).get();

        ASSERT_TRUE(std::dynamic_pointer_cast<
            one::helpers::buffering::BufferedFileHandle>(handle));

        handles.push_back(std::move(handle));
    }

    auto handle5 = helper->open("fileId" + std::to_string(5), 0, {}).get();

    // After the total memory limit for buffers is exhausted, BufferedAgent
    // should return non-buffered handles until some handle releases the
    // reserved memory
    ASSERT_FALSE(
        std::dynamic_pointer_cast<one::helpers::buffering::BufferedFileHandle>(
            handle5));

    // Destroy one handle
    handles.pop_back();

    auto handle6 = helper->open("fileId" + std::to_string(6), 0, {}).get();

    // After the total memory limit for buffers is exhausted, BufferedAgent
    // should return non-buffered handles until some handle releases the
    // reserved memory
    ASSERT_TRUE(
        std::dynamic_pointer_cast<one::helpers::buffering::BufferedFileHandle>(
            handle6));
}
