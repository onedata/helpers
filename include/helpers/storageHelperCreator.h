/**
 * @file storageHelperCreator.h
 * @author Rafal Slota
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_STORAGE_HELPER_FACTORY_H
#define HELPERS_STORAGE_HELPER_FACTORY_H

#include "storageHelper.h"

#ifdef BUILD_PROXY_IO
#include "communication/communicator.h"
#endif

#include <boost/optional.hpp>
#include <folly/executors/IOExecutor.h>
#include <tbb/concurrent_hash_map.h>

#include <memory>
#include <string>

namespace one {

class Scheduler;

namespace helpers {
namespace buffering {

class BufferAgentsMemoryLimitGuard;

namespace {
constexpr std::size_t kMB = 1024 * 1024;
constexpr std::size_t kReadBufferMinSize = 5 * kMB;
constexpr std::size_t kReadBufferMaxSize = 10 * kMB;
constexpr std::size_t kWriteBufferMinSize = 20 * kMB;
constexpr std::size_t kWriteBufferMaxSize = 50 * kMB;
constexpr int kWriteBufferFlushDelaySeconds = 5;
constexpr int kTargetLatencyNanoSeconds = 1000;
constexpr double kPrefetchPowerBase = 1.3;
} // namespace

struct BufferLimits {
    BufferLimits(std::size_t readBufferMinSize_ = kReadBufferMinSize,
        std::size_t readBufferMaxSize_ = kReadBufferMaxSize,
        std::chrono::seconds readBufferPrefetchDuration_ =
            std::chrono::seconds{1},
        std::size_t writeBufferMinSize_ = kWriteBufferMinSize,
        std::size_t writeBufferMaxSize_ = kWriteBufferMaxSize,
        std::chrono::seconds writeBufferFlushDelay_ =
            std::chrono::seconds{kWriteBufferFlushDelaySeconds},
        std::chrono::nanoseconds targetLatency_ =
            std::chrono::nanoseconds{kTargetLatencyNanoSeconds},
        double prefetchPowerBase_ = kPrefetchPowerBase,
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

/**
 * Factory providing objects of requested storage helpers.
 */
class StorageHelperCreator final {
public:
#ifdef BUILD_PROXY_IO
    StorageHelperCreator(
#if WITH_CEPH
        std::shared_ptr<folly::IOExecutor> cephExecutor,
        std::shared_ptr<folly::IOExecutor> cephRadosExecutor,
#endif
        std::shared_ptr<folly::IOExecutor> dioExecutor,
#if WITH_S3
        std::shared_ptr<folly::IOExecutor> s3Executor,
#endif
#if WITH_SWIFT
        std::shared_ptr<folly::IOExecutor> swiftExecutor,
#endif
#if WITH_GLUSTERFS
        std::shared_ptr<folly::IOExecutor> glusterfsExecutor,
#endif
#if WITH_WEBDAV
        std::shared_ptr<folly::IOExecutor> webDAVExecutor,
#endif
#if WITH_XROOTD
        std::shared_ptr<folly::IOExecutor> xrootdExecutor,
#endif
        std::shared_ptr<folly::IOExecutor> nullDeviceExecutor,
        communication::Communicator &m_communicator,
        std::size_t bufferSchedulerWorkers = 1,
        buffering::BufferLimits bufferLimits = buffering::BufferLimits{},
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);
#else
    StorageHelperCreator(
#if WITH_CEPH
        std::shared_ptr<folly::IOExecutor> cephExecutor,
        std::shared_ptr<folly::IOExecutor> cephRadosExecutor,
#endif
        std::shared_ptr<folly::IOExecutor> dioExecutor,
#if WITH_S3
        std::shared_ptr<folly::IOExecutor> s3Executor,
#endif
#if WITH_SWIFT
        std::shared_ptr<folly::IOExecutor> swiftExecutor,
#endif
#if WITH_GLUSTERFS
        std::shared_ptr<folly::IOExecutor> glusterfsExecutor,
#endif
#if WITH_WEBDAV
        std::shared_ptr<folly::IOExecutor> webDAVExecutor,
#endif
#if WITH_XROOTD
        std::shared_ptr<folly::IOExecutor> xrootdExecutor,
#endif
        std::shared_ptr<folly::IOExecutor> nullDeviceExecutor,
        std::size_t bufferSchedulerWorkers = 1,
        buffering::BufferLimits bufferLimits = buffering::BufferLimits{},
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);
#endif

    ~StorageHelperCreator();

    /**
     * Produces storage helper object.
     * @param name Name of storage helper that has to be returned.
     * @param args Arguments map passed as argument to storge helper's
     * constructor.
     * @param buffered Whether the storage helper should be wrapped
     * with a buffer agent.
     * @return The created storage helper object.
     */
    std::shared_ptr<StorageHelper> getStorageHelper(const folly::fbstring &name,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered,
        const std::unordered_map<folly::fbstring, folly::fbstring>
            &overrideParams = {});

    std::shared_ptr<StorageHelper> getStorageHelper(const folly::fbstring &name,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args)
    {
        return getStorageHelper(name, args, true);
    }

private:
#if WITH_CEPH
    std::shared_ptr<folly::IOExecutor> m_cephExecutor;
    std::shared_ptr<folly::IOExecutor> m_cephRadosExecutor;
#endif
    std::shared_ptr<folly::IOExecutor> m_dioExecutor;
#if WITH_S3
    std::shared_ptr<folly::IOExecutor> m_s3Executor;
#endif
#if WITH_SWIFT
    std::shared_ptr<folly::IOExecutor> m_swiftExecutor;
#endif
#if WITH_GLUSTERFS
    std::shared_ptr<folly::IOExecutor> m_glusterfsExecutor;
#endif
#if WITH_WEBDAV
    std::shared_ptr<folly::IOExecutor> m_webDAVExecutor;
#endif
#if WITH_XROOTD
    std::shared_ptr<folly::IOExecutor> m_xrootdExecutor;
#endif

    std::shared_ptr<folly::IOExecutor> m_nullDeviceExecutor;
    std::shared_ptr<Scheduler> m_scheduler;

    buffering::BufferLimits m_bufferLimits;
    std::shared_ptr<buffering::BufferAgentsMemoryLimitGuard>
        m_bufferMemoryLimitGuard;

#ifdef BUILD_PROXY_IO
    communication::Communicator &m_communicator;
#endif

    ExecutionContext m_executionContext;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_STORAGE_HELPER_FACTORY_H
