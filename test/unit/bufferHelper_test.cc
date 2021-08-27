#include "buffering/bufferAgent.h"
#include "helpers/storageHelper.h"
#include "scheduler.h"

#include <folly/FBString.h>
#include <folly/Singleton.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBufQueue.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>

class StorageHelper;

class FileHandle : public one::helpers::FileHandle {
public:
    FileHandle(const folly::fbstring &fileId,
        std::shared_ptr<folly::Executor> executor,
        std::atomic<bool> &wasSimultaneous,
        std::shared_ptr<StorageHelper> helper);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        one::helpers::WriteCallback &&writeCb) override;

    const one::helpers::Timeout &timeout() override;

private:
    std::shared_ptr<folly::Executor> m_executor;
    std::atomic<bool> &m_wasSimultaneous;
    std::atomic<size_t> m_simultaneous{0};
};

class StorageHelper : public one::helpers::StorageHelper,
                      public std::enable_shared_from_this<StorageHelper> {
public:
    StorageHelper(std::shared_ptr<folly::Executor> executor,
        std::atomic<bool> &wasSimultaneous)
        : m_executor{std::move(executor)}
        , m_wasSimultaneous{wasSimultaneous}
    {
    }

    folly::fbstring name() const override { return "teststorage"; }

    folly::Future<one::helpers::FileHandlePtr> open(
        const folly::fbstring &fileId, const int /*flags*/,
        const one::helpers::Params & /*openParams*/) override
    {
        return folly::makeFuture(static_cast<one::helpers::FileHandlePtr>(
            std::make_shared<FileHandle>(
                fileId, m_executor, m_wasSimultaneous, shared_from_this())));
    }

    const one::helpers::Timeout &timeout() override
    {
        return one::helpers::ASYNC_OPS_TIMEOUT;
    }

    std::shared_ptr<folly::Executor> executor() override { return m_executor; }

private:
    std::shared_ptr<folly::Executor> m_executor;
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

    one::helpers::buffering::BufferLimits limits{};
    std::shared_ptr<one::helpers::buffering::BufferAgentsMemoryLimitGuard>
        limitGuard;
};

// FileHandle implementation

FileHandle::FileHandle(const folly::fbstring &fileId,
    std::shared_ptr<folly::Executor> executor,
    std::atomic<bool> &wasSimultaneous, std::shared_ptr<StorageHelper> helper)
    : one::helpers::FileHandle{fileId, std::move(helper)}
    , m_executor{std::move(executor)}
    , m_wasSimultaneous{wasSimultaneous}
{
}

folly::Future<folly::IOBufQueue> FileHandle::read(
    const off_t offset, const std::size_t size)
{
    return folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
}

folly::Future<std::size_t> FileHandle::write(const off_t offset,
    folly::IOBufQueue buf, one::helpers::WriteCallback &&writeCb)
{
    return folly::via(m_executor.get(),
        [this, size = buf.chainLength(), writeCb = std::move(writeCb)] {
            if (++m_simultaneous > 1)
                m_wasSimultaneous = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            --m_simultaneous;
            if (writeCb)
                writeCb(size);
            return size;
        });
}

const one::helpers::Timeout &FileHandle::timeout()
{
    return one::helpers::ASYNC_OPS_TIMEOUT;
}

// FileHandle implementation

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
        scheduler = std::make_shared<one::Scheduler>(1);
        executor = std::make_shared<folly::IOThreadPoolExecutor>(100);

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

    ~BufferHelperTest() { }

    std::atomic<bool> wasSimultaneous{false};
    one::helpers::StorageHelperPtr helper;

private:
    std::shared_ptr<one::Scheduler> scheduler;
    std::shared_ptr<folly::IOThreadPoolExecutor> executor;
};

TEST_F(BufferHelperTest, shouldNotWriteSimultaneously)
{
    folly::SingletonVault::singleton()->registrationComplete();

    auto handle = helper->open("fileId", 0, {}).get();

    ASSERT_TRUE(
        std::dynamic_pointer_cast<one::helpers::buffering::BufferedFileHandle>(
            handle));

    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 1000; ++j) {
            folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
            buf.allocate(1024 * 1024);
            handle->write(j * 1024 * 1024, std::move(buf), {});
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
