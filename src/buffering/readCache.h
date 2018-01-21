/**
 * @file readCache.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_BUFFERING_READ_CACHE_H
#define HELPERS_BUFFERING_READ_CACHE_H

#include "communication/communicator.h"
#include "helpers/storageHelper.h"
#include "logging.h"
#include "messages/proxyio/remoteData.h"
#include "messages/proxyio/remoteRead.h"
#include "scheduler.h"

#include <folly/CallOnce.h>
#include <folly/FBString.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/io/IOBufQueue.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <queue>

namespace one {
namespace helpers {
namespace buffering {

class ReadCache : public std::enable_shared_from_this<ReadCache> {
    using FiberMutex = folly::fibers::TimedMutex;

    struct ReadData {
        ReadData(
            const off_t offset_, const std::size_t size_, const bool isPrefetch)
            : offset{offset_}
            , size{size_}
        {
            if (!isPrefetch)
                folly::call_once(measureLatencyFlag, [] {});
        }

        off_t offset;
        std::atomic<std::size_t> size;
        folly::once_flag measureLatencyFlag;
        folly::IOBufQueue buf;
        folly::SharedPromise<folly::Unit> promise;
    };

public:
    ReadCache(std::size_t readBufferMinSize, std::size_t readBufferMaxSize,
        std::chrono::seconds readBufferPrefetchDuration,
        double prefetchPowerBase, std::chrono::nanoseconds targetLatency,
        FileHandle &handle)
        : m_readBufferMinSize{readBufferMinSize}
        , m_readBufferMaxSize{readBufferMaxSize}
        , m_cacheDuration{readBufferPrefetchDuration * 2}
        , m_prefetchPowerBase{prefetchPowerBase}
        , m_targetLatency{static_cast<std::size_t>(targetLatency.count())}
        , m_handle{handle}
    {
        LOG_FCALL() << LOG_FARG(readBufferMinSize)
                    << LOG_FARG(readBufferMaxSize)
                    << LOG_FARG(readBufferPrefetchDuration.count());
    }

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size)
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

        std::unique_lock<FiberMutex> lock{m_mutex};
        if (isStale()) {
            LOG_DBG(2) << "Prefetch cache stalled for file "
                       << m_handle.fileId();
            while (!m_cache.empty()) {
                m_cache.pop();
            }
            m_clear = false;
        }

        if (isCurrentRead(offset))
            return readFromCache(offset, size);

        m_lastCacheRefresh = std::chrono::steady_clock::now();

        while (!m_cache.empty() && !isCurrentRead(offset))
            m_cache.pop();

        if (m_cache.empty()) {
            resetBlockSize();
            fetch(offset, size);
        }
        else {
            increaseBlockSize();
        }

        prefetchIfNeeded();

        return readFromCache(offset, size);
    }

    void clear()
    {
        LOG_FCALL();

        std::unique_lock<FiberMutex> lock{m_mutex};
        m_clear = true;
    }

private:
    void prefetchIfNeeded()
    {
        LOG_FCALL();

        assert(!m_cache.empty());

        if (m_cache.size() < 2 && m_blockSize > 0) {
            const auto nextOffset =
                m_cache.back()->offset + m_cache.back()->size;

            LOG_DBG(2) << "Prefetching " << m_blockSize << " bytes for file "
                       << m_handle.fileId() << " at offset " << nextOffset;

            prefetch(nextOffset, m_blockSize);
        }
    }

    void prefetch(const off_t offset, const std::size_t size)
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

        fetch(offset, size, true);
    }

    void fetch(const off_t offset, const std::size_t size)
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

        fetch(offset, size, false);
    }

    void fetch(const off_t offset, const std::size_t size, bool isPrefetch)
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size)
                    << LOG_FARG(isPrefetch);

        m_cache.emplace(std::make_shared<ReadData>(offset, size, isPrefetch));
        m_handle.read(offset, size)
            .then([readData = m_cache.back()](folly::IOBufQueue buf) {
                readData->size = buf.chainLength();
                readData->buf = std::move(buf);
                readData->promise.setValue();
            })
            .onError([readData = m_cache.back()](folly::exception_wrapper ew) {
                readData->promise.setException(std::move(ew));
            });
    }

    folly::Future<folly::IOBufQueue> readFromCache(
        const off_t offset, const std::size_t size)
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

        assert(!m_cache.empty());

        auto readData = m_cache.front();
        const auto startPoint = std::chrono::steady_clock::now();
        return readData->promise.getFuture().then([ =, s = weak_from_this() ] {
            folly::call_once(readData->measureLatencyFlag, [&] {
                if (auto self = s.lock()) {
                    const auto latency =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - startPoint)
                            .count();

                    LOG_DBG(2)
                        << "Latest measured read latency for "
                        << m_handle.fileId() << " is " << m_latency << " ns";

                    m_latency = (m_latency + 2 * latency) / 3;

                    LOG_DBG(2)
                        << "Adjusted average read latency for "
                        << m_handle.fileId() << " to " << m_latency << " ns";
                }
            });

            folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
            if (readData->buf.empty() ||
                static_cast<off_t>(
                    readData->offset + readData->buf.chainLength()) < offset) {
                LOG_DBG(2) << "Latest block in read cache is empty or outside "
                              "requested range for file "
                           << m_handle.fileId();
                return buf;
            }

            buf.append(readData->buf.front()->clone());
            if (offset > readData->offset) {
                LOG_DBG(2) << "Trimming latest read cache block for file "
                           << m_handle.fileId()
                           << " to start at requested offset by: "
                           << offset - readData->offset;
                buf.trimStart(offset - readData->offset);
            }
            if (buf.chainLength() > size) {
                LOG_DBG(2) << "Trimming latest read cache block for file "
                           << m_handle.fileId()
                           << " to end at requested size by: "
                           << buf.chainLength() - size;
                buf.trimEnd(buf.chainLength() - size);
            }

            return buf;
        });
    }

    bool isCurrentRead(const off_t offset)
    {
        return !m_cache.empty() && m_cache.front()->offset <= offset &&
            offset <
            static_cast<off_t>(m_cache.front()->offset + m_cache.front()->size);
    }

    bool isStale() const
    {
        return m_clear ||
            m_lastCacheRefresh + m_cacheDuration <
            std::chrono::steady_clock::now();
    }

    void resetBlockSize()
    {
        LOG_FCALL();

        m_blockSize = 0;
        m_prefetchCoeff = 1;
    }

    void increaseBlockSize()
    {
        LOG_FCALL();

        if (m_blockSize < m_readBufferMaxSize &&
            m_latency.load() > m_targetLatency) {
            m_blockSize = std::min(
                m_readBufferMaxSize, m_readBufferMinSize * m_prefetchCoeff);
            m_prefetchCoeff *= m_prefetchPowerBase;

            LOG_DBG(2) << "Adjusted prefetch block size for file "
                       << m_handle.fileId() << " to: " << m_blockSize
                       << " and prefetch coefficient to: " << m_prefetchCoeff;
        }
    }

    std::weak_ptr<ReadCache> weak_from_this() { return {shared_from_this()}; }

    const std::size_t m_readBufferMinSize;
    const std::size_t m_readBufferMaxSize;
    const std::chrono::seconds m_cacheDuration;
    const double m_prefetchPowerBase;
    const std::size_t m_targetLatency;
    FileHandle &m_handle;

    std::size_t m_prefetchCoeff{0};
    std::size_t m_blockSize{0};
    std::atomic<std::size_t> m_latency{m_targetLatency};

    FiberMutex m_mutex;
    std::queue<std::shared_ptr<ReadData>> m_cache;
    bool m_clear{false};
    std::chrono::steady_clock::time_point m_lastCacheRefresh{};
};

} // namespace proxyio
} // namespace helpers
} // namespace one

#endif // HELPERS_BUFFERING_READ_CACHE_H
