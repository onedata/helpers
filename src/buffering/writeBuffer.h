/**
 * @file writeBuffer.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_BUFFERING_WRITE_BUFFER_H
#define HELPERS_BUFFERING_WRITE_BUFFER_H

#include "readCache.h"

#include "communication/communicator.h"
#include "helpers/logging.h"
#include "helpers/storageHelper.h"
#include "messages/proxyio/remoteWrite.h"
#include "messages/proxyio/remoteWriteResult.h"
#include "scheduler.h"

#include <folly/FBString.h>
#include <folly/FBVector.h>
#include <folly/fibers/Baton.h>

#if defined(__APPLE__)
// There is no spinlock on OSX and Folly TimedMutex doesn't have an ifded
// to detect this.
typedef pthread_rwlock_t pthread_spinlock_t;
#endif

#include <folly/fibers/TimedMutex.h>
#include <folly/futures/Future.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace one {
namespace helpers {
namespace buffering {

class WriteBuffer : public std::enable_shared_from_this<WriteBuffer> {
    using FiberMutex = folly::fibers::TimedMutex;

public:
    WriteBuffer(const std::size_t writeBufferMinSize,
        const std::size_t writeBufferMaxSize,
        const std::chrono::seconds writeBufferFlushDelay, FileHandle &handle,
        std::shared_ptr<Scheduler> scheduler,
        std::shared_ptr<ReadCache> readCache)
        : m_writeBufferMinSize {writeBufferMinSize}
        , m_writeBufferMaxSize {writeBufferMaxSize}
        , m_writeBufferFlushDelay {writeBufferFlushDelay}
        , m_handle {handle}
        , m_scheduler {std::move(scheduler)}
        , m_readCache {std::move(readCache)}
        , m_writeFuture {handle.helper()->executor().get()}
    {
        LOG_FCALL() << LOG_FARG(writeBufferMinSize)
                    << LOG_FARG(writeBufferMaxSize)
                    << LOG_FARG(writeBufferFlushDelay.count());
    }

    ~WriteBuffer()
    {
        LOG_FCALL();
        m_cancelFlushSchedule();
    }

    folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

        using one::logging::log_timer;
        using one::logging::csv::log;
        using one::logging::csv::read_write_perf;

        log_timer<> timer;

        std::unique_lock<FiberMutex> lock {m_mutex};

        m_cancelFlushSchedule();
        scheduleFlush();

        const std::size_t size = buf.chainLength();

        // If the new data block is not a continuation of the last block in the
        // buffer queue, add it separatel, otherwise append the data to the
        // last block in the queue
        if (m_buffers.empty() || m_nextOffset != offset)
            m_buffers.emplace_back(offset, std::move(buf), std::move(writeCb));
        else
            // This assumes that the consecutive blocks are on the same storage
            // and so the write callback can be reused
            std::get<1>(m_buffers.back()).append(std::move(buf));

        m_bufferedSize += size;
        m_nextOffset = offset + size;

        if (m_bufferedSize > calculateFlushThreshold()) {
            // We're always returning "everything" on success, so provider has
            // to try to save everything and return an error if not successful.
            pushBuffer();

            return confirmOverThreshold().thenValue(
                [fileId = m_handle.fileId(), size, offset, timer](
                    auto && /*unit*/) {
                    log<read_write_perf>(fileId, "WriteBuffer", "write", offset,
                        size, timer.stop());
                    return size;
                });
        }

        log<read_write_perf>(m_handle.fileId(), "WriteBuffer", "write", offset,
            size, timer.stop());

        return folly::makeFuture(size);
    }

    folly::Future<folly::Unit> fsync()
    {
        LOG_FCALL();

        std::unique_lock<FiberMutex> lock {m_mutex};
        pushBuffer();
        return confirmAll();
    }

    void scheduleFlush()
    {
        LOG_FCALL();

        m_cancelFlushSchedule = m_scheduler->schedule(m_writeBufferFlushDelay,
            [s = std::weak_ptr<WriteBuffer>(shared_from_this())] {
                if (auto self = s.lock()) {
                    std::unique_lock<FiberMutex> lock {self->m_mutex};
                    self->pushBuffer();
                    self->scheduleFlush();
                }
            });
    }

private:
    void pushBuffer()
    {
        LOG_FCALL();

        if (m_bufferedSize == 0)
            return;

        decltype(m_buffers) buffers;
        buffers.swap(m_buffers);

        auto sentSize = m_bufferedSize;
        m_pendingConfirmation += sentSize;
        m_bufferedSize = 0;

        const auto startPoint = std::chrono::steady_clock::now();

        auto confirmationPromise =
            std::make_shared<folly::Promise<folly::Unit>>();

        using one::logging::log_timer;
        using one::logging::csv::log;
        using one::logging::csv::read_write_perf;

        log_timer<> timer;

        m_writeFuture =
            std::move(m_writeFuture)
                .thenValue([s = std::weak_ptr<WriteBuffer>(shared_from_this()),
                               buffers = std::move(buffers)](
                               auto && /*unit*/) mutable {
                    if (auto self = s.lock())
                        return self->m_handle.multiwrite(std::move(buffers));

                    return folly::makeFuture<std::size_t>(std::system_error {
                        std::make_error_code(std::errc::owner_dead)});
                })
                .thenValue([startPoint, sentSize, fileId = m_handle.fileId(),
                               timer,
                               s = std::weak_ptr<WriteBuffer>(
                                   shared_from_this())](auto && /*unused*/) {
                    auto self = s.lock();
                    if (!self)
                        return;

                    auto duration =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - startPoint)
                            .count();

                    if (duration > 0) {
                        constexpr auto kBillion = 1'000'000'000ULL;
                        auto bandwidth = sentSize * kBillion / duration;
                        self->m_bps = (self->m_bps * 1 + bandwidth * 2) / 3;
                    }

                    self->m_readCache->clear();

                    log<read_write_perf>(fileId, "WriteBuffer", "pushBuffers",
                        "-", sentSize, timer.stop());
                })
                .thenValue([confirmationPromise](auto && /*unit*/) {
                    confirmationPromise->setValue();
                })
                .thenError(folly::tag_t<std::system_error> {},
                    [confirmationPromise](auto &&e) {
                        confirmationPromise->setException(
                            std::forward<decltype(e)>(e));
                    })
                .thenError(folly::tag_t<folly::exception_wrapper> {},
                    [confirmationPromise](auto &&ew) {
                        confirmationPromise->setException(
                            std::forward<decltype(ew)>(ew));
                    });

        m_confirmationFutures.emplace(std::make_pair(sentSize,
            confirmationPromise->getSemiFuture().via(
                m_handle.helper()->executor().get())));
    }

    folly::Future<folly::Unit> confirmOverThreshold()
    {
        LOG_FCALL();

        return confirm(calculateConfirmThreshold());
    }

    folly::Future<folly::Unit> confirmAll()
    {
        LOG_FCALL();
        return confirm(0);
    }

    folly::Future<folly::Unit> confirm(const std::size_t threshold)
    {
        LOG_FCALL() << LOG_FARG(threshold);

        folly::fbvector<folly::Future<folly::Unit>> confirmFutures;

        while (m_pendingConfirmation > threshold) {
            confirmFutures.emplace_back(
                std::move(m_confirmationFutures.front().second));

            m_pendingConfirmation -= m_confirmationFutures.front().first;
            m_confirmationFutures.pop();
        }

        return folly::collectAll(std::move(confirmFutures))
            .via(m_handle.helper()->executor().get())
            .thenValue([](std::vector<folly::Try<folly::Unit>> &&tries) {
                for (const auto &t : tries) {
                    if (t.hasException()) {
                        return folly::makeFuture<folly::Unit>(t.exception());
                    }
                }

                return folly::makeFuture();
            });
    }

    std::size_t calculateFlushThreshold()
    {
        return std::min(
            m_writeBufferMaxSize, std::max(m_writeBufferMinSize, 2 * m_bps));
    }

    std::size_t calculateConfirmThreshold()
    {
        constexpr auto kThresholdMultiplier = 6;
        return kThresholdMultiplier * calculateFlushThreshold();
    }

    std::size_t m_writeBufferMinSize;
    std::size_t m_writeBufferMaxSize;
    std::chrono::seconds m_writeBufferFlushDelay;

    FileHandle &m_handle;
    std::shared_ptr<Scheduler> m_scheduler;
    std::shared_ptr<ReadCache> m_readCache;

    std::function<void()> m_cancelFlushSchedule;

    std::size_t m_bufferedSize = 0;
    off_t m_nextOffset = 0;
    folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>>
        m_buffers;
    std::atomic<std::size_t> m_bps {0};

    FiberMutex m_mutex;
    std::size_t m_pendingConfirmation = 0;
    folly::Future<folly::Unit> m_writeFuture;
    std::queue<std::pair<off_t, folly::Future<folly::Unit>>>
        m_confirmationFutures;
};

} // namespace buffering
} // namespace helpers
} // namespace one

#endif // HELPERS_BUFFERING_WRITE_BUFFER_H
