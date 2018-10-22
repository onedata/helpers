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

#include <asio/io_service.hpp>
#include <boost/optional.hpp>
#include <tbb/concurrent_hash_map.h>

#ifdef WITH_WEBDAV
#include <folly/executors/IOExecutor.h>
#endif

#include <memory>
#include <string>

namespace one {

class Scheduler;

namespace helpers {

#if WITH_CEPH
constexpr auto CEPH_HELPER_NAME = "ceph";
constexpr auto CEPHRADOS_HELPER_NAME = "cephrados";
#endif

constexpr auto POSIX_HELPER_NAME = "posix";

constexpr auto PROXY_HELPER_NAME = "proxy";

constexpr auto NULL_DEVICE_HELPER_NAME = "nulldevice";

#if WITH_S3
constexpr auto S3_HELPER_NAME = "s3";
#endif

#if WITH_SWIFT
constexpr auto SWIFT_HELPER_NAME = "swift";
#endif

#if WITH_GLUSTERFS
constexpr auto GLUSTERFS_HELPER_NAME = "glusterfs";
#endif

#if WITH_WEBDAV
constexpr auto WEBDAV_HELPER_NAME = "webdav";
#endif

namespace buffering {

class BufferAgentsMemoryLimitGuard;

struct BufferLimits {
    BufferLimits(std::size_t readBufferMinSize_ = 5 * 1024 * 1024,
        std::size_t readBufferMaxSize_ = 10 * 1024 * 1024,
        std::chrono::seconds readBufferPrefetchDuration_ =
            std::chrono::seconds{1},
        std::size_t writeBufferMinSize_ = 20 * 1024 * 1024,
        std::size_t writeBufferMaxSize_ = 50 * 1024 * 1024,
        std::chrono::seconds writeBufferFlushDelay_ = std::chrono::seconds{5},
        std::chrono::nanoseconds targetLatency_ =
            std::chrono::nanoseconds{1000},
        double prefetchPowerBase_ = 1.3, std::size_t readBuffersTotalSize_ = 0,
        std::size_t writeBuffersTotalSize_ = 0)
        : readBufferMinSize{readBufferMinSize_}
        , readBufferMaxSize{readBufferMaxSize_}
        , readBuffersTotalSize{readBuffersTotalSize_}
        , prefetchPowerBase{prefetchPowerBase_}
        , targetLatency{std::move(targetLatency_)}
        , readBufferPrefetchDuration{std::move(readBufferPrefetchDuration_)}
        , writeBufferMinSize{writeBufferMinSize_}
        , writeBufferMaxSize{writeBufferMaxSize_}
        , writeBuffersTotalSize{writeBuffersTotalSize_}
        , writeBufferFlushDelay{std::move(writeBufferFlushDelay_)}
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
class StorageHelperCreator {
public:
#ifdef BUILD_PROXY_IO
    StorageHelperCreator(
#if WITH_CEPH
        asio::io_service &cephService, asio::io_service &cephRadosService,
#endif
        asio::io_service &dioService,
#if WITH_S3
        asio::io_service &kvS3Service,
#endif
#if WITH_SWIFT
        asio::io_service &kvSwiftService,
#endif
#if WITH_GLUSTERFS
        asio::io_service &glusterfsService,
#endif
#if WITH_WEBDAV
        std::shared_ptr<folly::IOExecutor> webDAVExecutor,
#endif
        asio::io_service &nullDeviceService,
        communication::Communicator &m_communicator,
        std::size_t bufferSchedulerWorkers = 1,
        buffering::BufferLimits bufferLimits = buffering::BufferLimits{});
#else
    StorageHelperCreator(
#if WITH_CEPH
        asio::io_service &cephService, asio::io_service &cephRadosService,
#endif
        asio::io_service &dioService,
#if WITH_S3
        asio::io_service &kvS3Service,
#endif
#if WITH_SWIFT
        asio::io_service &kvSwiftService,
#endif
#if WITH_GLUSTERFS
        asio::io_service &glusterfsService,
#endif
#if WITH_WEBDAV
        std::shared_ptr<folly::IOExecutor> webDAVExecutor,
#endif
        asio::io_service &nullDeviceService,
        std::size_t bufferSchedulerWorkers = 1,
        buffering::BufferLimits bufferLimits = buffering::BufferLimits{});
#endif

    virtual ~StorageHelperCreator();

    /**
     * Produces storage helper object.
     * @param name Name of storage helper that has to be returned.
     * @param args Arguments map passed as argument to storge helper's
     * constructor.
     * @param buffered Whether the storage helper should be wrapped
     * with a buffer agent.
     * @return The created storage helper object.
     */
    virtual std::shared_ptr<StorageHelper> getStorageHelper(
        const folly::fbstring &name,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered);

    std::shared_ptr<StorageHelper> getStorageHelper(const folly::fbstring &name,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args)
    {
        return getStorageHelper(name, args, true);
    }

private:
#if WITH_CEPH
    asio::io_service &m_cephService;
    asio::io_service &m_cephRadosService;
#endif
    asio::io_service &m_dioService;
#if WITH_S3
    asio::io_service &m_s3Service;
#endif
#if WITH_SWIFT
    asio::io_service &m_swiftService;
#endif
#if WITH_GLUSTERFS
    asio::io_service &m_glusterfsService;
#endif
#if WITH_WEBDAV
    std::shared_ptr<folly::IOExecutor> m_webDAVExecutor;
#endif

    asio::io_service &m_nullDeviceService;
    std::unique_ptr<Scheduler> m_scheduler;

    buffering::BufferLimits m_bufferLimits;
    std::shared_ptr<buffering::BufferAgentsMemoryLimitGuard>
        m_bufferMemoryLimitGuard;

#ifdef BUILD_PROXY_IO
    communication::Communicator &m_communicator;
#endif
};

} // namespace helpers
} // namespace one

#endif // HELPERS_STORAGE_HELPER_FACTORY_H
