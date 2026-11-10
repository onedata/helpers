/**
 * @file bufferLimits.h
 * @author Bartek Kryza
 * @copyright (C) 2023 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_BUFFER_LIMITS_H
#define HELPERS_BUFFER_LIMITS_H

#include <chrono>

namespace one {
namespace helpers {
namespace buffering {
namespace constants {
constexpr std::size_t kMB{1024 * 1024};
constexpr std::size_t kReadBufferMinSize = 5 * kMB;
constexpr std::size_t kReadBufferMaxSize = 10 * kMB;
constexpr std::size_t kWriteBufferMinSize = 20 * kMB;
constexpr std::size_t kWriteBufferMaxSize = 50 * kMB;
constexpr int kWriteBufferFlushDelaySeconds = 5;
constexpr int kTargetLatencyNanoSeconds = 1000;
constexpr double kPrefetchPowerBase = 1.3;
} // namespace constants

struct BufferLimits {
    explicit BufferLimits(
        std::size_t readBufferMinSize_ = constants::kReadBufferMinSize,
        std::size_t readBufferMaxSize_ = constants::kReadBufferMaxSize,
        std::chrono::seconds readBufferPrefetchDuration_ =
            std::chrono::seconds{1},
        std::size_t writeBufferMinSize_ = constants::kWriteBufferMinSize,
        std::size_t writeBufferMaxSize_ = constants::kWriteBufferMaxSize,
        std::chrono::seconds writeBufferFlushDelay_ =
            std::chrono::seconds{constants::kWriteBufferFlushDelaySeconds},
        std::chrono::nanoseconds targetLatency_ =
            std::chrono::nanoseconds{constants::kTargetLatencyNanoSeconds},
        double prefetchPowerBase_ = constants::kPrefetchPowerBase,
        std::size_t readBuffersTotalSize_ = 0,
        std::size_t writeBuffersTotalSize_ = 0)
        : readBufferMinSize{readBufferMinSize_}
        , readBufferMaxSize{readBufferMaxSize_}
        , readBuffersTotalSize{readBuffersTotalSize_}
        , prefetchPowerBase{prefetchPowerBase_}
        , targetLatency{targetLatency_}
        , readBufferPrefetchDuration{readBufferPrefetchDuration_}
        , writeBufferMinSize{writeBufferMinSize_}
        , writeBufferMaxSize{writeBufferMaxSize_}
        , writeBuffersTotalSize{writeBuffersTotalSize_}
        , writeBufferFlushDelay{writeBufferFlushDelay_}
    {
    }

    /**
     * @name Variables impacting prefetching.
     * The prefetched block size is never smaller than @c readBufferMinSize or
     * bigger than @c readBufferMaxSize . The actual formula for prefetched
     * block size is:
     * @code
     * readBufferMinSize * prefetchPowerBase ^ number_of_subsequent_hits
     * @endcode
     * When the number of subsequent hits (i.e. subsequent requested blocks for
     * reading) is 0, no prefetching is done.
     * The latency between first request to read from a prefetched block and
     * the actual reading from the prefetched block is also measured, and once
     * the latency falls below @c targetLatency the block size is no longer
     * expanded.
     * The data is kept in the cache at most 2 * @c readBufferPrefetchDuration
     * after which the whole cache is considered stale and dropped. Each time a
     * block starts to be used (i.e. switches state from "prefetched" to "used")
     * the staleness timer is reset.
     */
    ///@{
    std::size_t readBufferMinSize;
    std::size_t readBufferMaxSize;
    std::size_t readBuffersTotalSize;
    double prefetchPowerBase;
    std::chrono::nanoseconds targetLatency;
    std::chrono::seconds readBufferPrefetchDuration;
    ///@}

    /**
     * @name Variables impacting output buffering.
     */
    ///@{
    std::size_t writeBufferMinSize;
    std::size_t writeBufferMaxSize;
    std::size_t writeBuffersTotalSize;
    std::chrono::seconds writeBufferFlushDelay;
    ///@}
};

} // namespace buffering
} // namespace helpers
} // namespace one

#endif // HELPERS_BUFFER_LIMITS_H