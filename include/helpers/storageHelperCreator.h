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

#include "versionedStorageHelper.h"

#ifndef WITH_BUFFERING
#ifdef BUILD_PROXY_IO
#define WITH_BUFFERING
#endif
#endif

#include "bufferedStorageHelper.h"
#ifdef WITH_BUFFERING
#include "buffering/bufferAgent.h"
#include "buffering/bufferLimits.h"
#endif
#include "helpers/logging.h"
#include "nullDeviceHelper.h"
#include "posixHelper.h"
#ifdef BUILD_PROXY_IO
#include "proxyHelper.h"
#endif
#include "scheduler.h"
#include "storageFanInHelper.h"
#include "storageRouterHelper.h"

#if WITH_CEPH
#include "cephHelper.h"
#include "cephRadosHelper.h"
#endif

#if WITH_S3
#include "s3Helper.h"
#endif

#if WITH_SWIFT
#include "swiftHelper.h"
#endif

#if WITH_GLUSTERFS
#include "glusterfsHelper.h"
#endif

#if WITH_WEBDAV
#include "httpHelper.h"
#include "webDAVHelper.h"
#endif

#if WITH_XROOTD
#include "xrootdHelper.h"
#endif

#if WITH_NFS
#include "nfsHelper.h"
#endif

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
#ifdef WITH_BUFFERING
namespace buffering {
class BufferAgentsMemoryLimitGuard;
} // namespace buffering
#endif

/**
 * Factory providing objects of requested storage helpers.
 */
template <typename CommunicatorT> class StorageHelperCreator final {
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
#if WITH_NFS
        std::shared_ptr<folly::IOExecutor> nfsExecutor,
#endif
        std::shared_ptr<folly::IOExecutor> nullDeviceExecutor,
        CommunicatorT &m_communicator,
#ifdef WITH_BUFFERING
        std::size_t bufferSchedulerWorkers = 1,
        buffering::BufferLimits bufferLimits = buffering::BufferLimits{},
#endif // WITH_BUFFERING
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);
#else // BUILD_PROXY_IO
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
#if WITH_NFS
        std::shared_ptr<folly::IOExecutor> nfsExecutor,
#endif
        std::shared_ptr<folly::IOExecutor> nullDeviceExecutor,
#ifdef WITH_BUFFERING
        std::size_t bufferSchedulerWorkers = 1,
        buffering::BufferLimits bufferLimits = buffering::BufferLimits{},
#endif // WITH_BUFFERING
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);
#endif

    ~StorageHelperCreator() = default;

    /**
     * Produces storage helper object.
     * @param type Name of storage helper that has to be returned.
     * @param args Arguments map passed as argument to storge helper's
     * constructor.
     * @param buffered Whether the storage helper should be wrapped
     * with a buffer agent.
     * @return The created storage helper object.
     */

    std::shared_ptr<StorageHelper> getStorageHelper(const folly::fbstring &type,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered,
        const std::unordered_map<folly::fbstring, folly::fbstring>
            &overrideParams = {})
    {
        return std::make_shared<
            VersionedStorageHelper<StorageHelperCreator<CommunicatorT>>>(*this,
            getStorageHelperInternal(type, args, buffered, overrideParams));
    }

    std::shared_ptr<StorageHelper> getStorageHelper(
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered)
    {
        return getStorageHelper(args.at("type"), args, buffered);
    }

    std::shared_ptr<StorageHelper> getRawStorageHelper(
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered)
    {
        return getStorageHelperInternal(args.at("type"), args, buffered);
    }

private:
    std::shared_ptr<StorageHelper> getStorageHelperInternal(
        const folly::fbstring &name,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered,
        const std::unordered_map<folly::fbstring, folly::fbstring>
            &overrideParams = {});

    std::vector<StorageHelperPtr> splitMultipleSupportNFSStorages(
        const std::unordered_map<folly::fbstring, folly::fbstring>
            &overrideParams,
        folly::fbstring &host, folly::fbstring &volume) const;

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
#if WITH_NFS
    std::shared_ptr<folly::IOExecutor> m_nfsExecutor;
#endif

    std::shared_ptr<folly::IOExecutor> m_nullDeviceExecutor;
    std::shared_ptr<Scheduler> m_scheduler;

#ifdef WITH_BUFFERING
    buffering::BufferLimits m_bufferLimits;
    std::shared_ptr<buffering::BufferAgentsMemoryLimitGuard>
        m_bufferMemoryLimitGuard;
#endif

#ifdef BUILD_PROXY_IO
    CommunicatorT &m_communicator;
#endif

    ExecutionContext m_executionContext;
};

constexpr std::size_t kProxyHelperMaximumReadBufferSize = 52'428'800;
constexpr std::size_t kProxyHelperMaximumWriteBufferSize = 52'428'800;

#ifdef BUILD_PROXY_IO
template <typename CommunicatorT>
StorageHelperCreator<CommunicatorT>::StorageHelperCreator(
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
#if WITH_NFS
    std::shared_ptr<folly::IOExecutor> nfsExecutor,
#endif
    std::shared_ptr<folly::IOExecutor> nullDeviceExecutor,
    CommunicatorT &communicator, std::size_t bufferSchedulerWorkers,
    buffering::BufferLimits bufferLimits, ExecutionContext executionContext)
    :
#if WITH_CEPH
    m_cephExecutor{std::move(cephExecutor)}
    , m_cephRadosExecutor{std::move(cephRadosExecutor)}
    ,
#endif
    m_dioExecutor{std::move(dioExecutor)}
    ,
#if WITH_S3
    m_s3Executor{std::move(s3Executor)}
    ,
#endif
#if WITH_SWIFT
    m_swiftExecutor{std::move(swiftExecutor)}
    ,
#endif
#if WITH_GLUSTERFS
    m_glusterfsExecutor{std::move(glusterfsExecutor)}
    ,
#endif
#if WITH_WEBDAV
    m_webDAVExecutor{std::move(webDAVExecutor)}
    ,
#endif
#if WITH_XROOTD
    m_xrootdExecutor{std::move(xrootdExecutor)}
    ,
#endif
#if WITH_NFS
    m_nfsExecutor{std::move(nfsExecutor)}
    ,
#endif
    m_nullDeviceExecutor{std::move(nullDeviceExecutor)}
    , m_scheduler{std::make_unique<Scheduler>(bufferSchedulerWorkers)}
    , m_bufferLimits{bufferLimits}
    , m_bufferMemoryLimitGuard{std::make_shared<
          buffering::BufferAgentsMemoryLimitGuard>(bufferLimits)}
    , m_communicator{communicator}
    , m_executionContext{executionContext}
{
}
#else // BUILD_PROXY_IO

template <typename CommunicatorT>
StorageHelperCreator<CommunicatorT>::StorageHelperCreator(
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
#if WITH_NFS
    std::shared_ptr<folly::IOExecutor> nfsExecutor,
#endif
    std::shared_ptr<folly::IOExecutor> nullDeviceExecutor,
#ifdef WITH_BUFFERING
    std::size_t bufferSchedulerWorkers, buffering::BufferLimits bufferLimits,
#endif
    ExecutionContext executionContext)
    :
#if WITH_CEPH
    m_cephExecutor{std::move(cephExecutor)}
    , m_cephRadosExecutor{std::move(cephRadosExecutor)}
    ,
#endif
    m_dioExecutor{std::move(dioExecutor)}
    ,
#if WITH_S3
    m_s3Executor{std::move(s3Executor)}
    ,
#endif
#if WITH_SWIFT
    m_swiftExecutor{std::move(swiftExecutor)}
    ,
#endif
#if WITH_GLUSTERFS
    m_glusterfsExecutor{std::move(glusterfsExecutor)}
    ,
#endif
#if WITH_WEBDAV
    m_webDAVExecutor{std::move(webDAVExecutor)}
    ,
#endif
#if WITH_XROOTD
    m_xrootdExecutor{std::move(xrootdExecutor)}
    ,
#endif
#if WITH_NFS
    m_nfsExecutor{std::move(nfsExecutor)}
    ,
#endif
    m_nullDeviceExecutor{std::move(nullDeviceExecutor)}
    , m_executionContext{executionContext}
{
}
#endif

template <typename CommunicatorT>
std::shared_ptr<StorageHelper>
StorageHelperCreator<CommunicatorT>::getStorageHelperInternal(
    const folly::fbstring &name,
    const std::unordered_map<folly::fbstring, folly::fbstring> &args,
    const bool buffered,
    const std::unordered_map<folly::fbstring, folly::fbstring> &overrideParams)
{
    LOG_FCALL() << LOG_FARG(name) << LOG_FARGM(args) << LOG_FARG(buffered);

    StorageHelperPtr helper;

    if (name == STORAGE_ROUTER_HELPER_NAME) {
        helper = StorageRouterHelperFactory{}.createStorageHelper(
            args, m_executionContext);
    }

    if (name == POSIX_HELPER_NAME) {
        auto mountPoint = getParam<folly::fbstring>(args, "mountPoint");
        if (mountPoint.find(':') == std::string::npos) {
            // This is a regular posix helper
            helper = PosixHelperFactory{m_dioExecutor}
                         .createStorageHelperWithOverride(
                             args, overrideParams, m_executionContext);
        }
        else {
            // This is a multi-posix helper which handles multiple mountpoints
            // in parallel
            if (overrideParams.find("mountPoint") != overrideParams.cend())
                mountPoint =
                    getParam<folly::fbstring>(overrideParams, "mountPoint");

            std::vector<std::string> mountPoints;
            folly::split(":", mountPoint, mountPoints, true);
            std::vector<StorageHelperPtr> storages;
            auto factory = PosixHelperFactory{m_dioExecutor};
            for (const auto &mp : mountPoints) {
                Params argsCopy;
                argsCopy["mountPoint"] = mp;
                storages.emplace_back(factory.createStorageHelperWithOverride(
                    argsCopy, overrideParams, m_executionContext));
            }
            helper = std::make_shared<StorageFanInHelper>(
                std::move(storages), m_dioExecutor, m_executionContext);
        }
    }

#if WITH_CEPH
    if (name == CEPH_HELPER_NAME)
        helper =
            CephHelperFactory{m_cephExecutor}.createStorageHelperWithOverride(
                args, overrideParams, m_executionContext);

    if (name == CEPHRADOS_HELPER_NAME)
        helper = CephRadosHelperFactory{m_cephRadosExecutor}
                     .createStorageHelperWithOverride(
                         args, overrideParams, m_executionContext);
#endif

#ifdef BUILD_PROXY_IO
    if (name == PROXY_HELPER_NAME)
        helper = ProxyHelperFactory<CommunicatorT>{m_communicator}
                     .createStorageHelperWithOverride(
                         args, overrideParams, m_executionContext);
#endif

#if WITH_S3
    if (name == S3_HELPER_NAME) {
        if (getParam<bool>(args, "archiveStorage", false)) {
            const auto kDefaultBufferStorageBlockSizeMultiplier = 5UL;
            auto bufferArgs{args};
            auto mainArgs{args};
            auto bufferedArgs{args};
            bufferArgs["blockSize"] =
                std::to_string(kDefaultBufferStorageBlockSizeMultiplier *
                    getParam<std::size_t>(
                        args, "blockSize", constants::DEFAULT_BLOCK_SIZE));
            mainArgs["storagePathType"] = "canonical";
            bufferedArgs["bufferPath"] = ".__onedata__buffer";
            bufferedArgs["bufferDepth"] = "2";

            auto bufferHelper =
                S3HelperFactory{m_s3Executor}.createStorageHelperWithOverride(
                    bufferArgs, overrideParams, m_executionContext);
            auto mainHelper =
                S3HelperFactory{m_s3Executor}.createStorageHelperWithOverride(
                    mainArgs, overrideParams, m_executionContext);
            auto bufferedHelper =
                BufferedStorageHelperFactory{}.createStorageHelper(
                    std::move(bufferHelper), std::move(mainHelper),
                    bufferedArgs, m_executionContext);

            std::map<folly::fbstring, StorageHelperPtr> routes;
            routes["/.__onedata__archive"] = std::move(bufferedHelper);
            routes["/"] =
                S3HelperFactory{m_s3Executor}.createStorageHelperWithOverride(
                    args, overrideParams, m_executionContext);
            helper = std::make_shared<StorageRouterHelper>(
                std::move(routes), m_executionContext);
        }
        else
            helper =
                S3HelperFactory{m_s3Executor}.createStorageHelperWithOverride(
                    args, overrideParams, m_executionContext);
    }
#endif

#if WITH_SWIFT
    if (name == SWIFT_HELPER_NAME)
        helper =
            SwiftHelperFactory{m_swiftExecutor}.createStorageHelperWithOverride(
                args, overrideParams, m_executionContext);
#endif

#if WITH_GLUSTERFS
    if (name == GLUSTERFS_HELPER_NAME)
        helper = GlusterFSHelperFactory{m_glusterfsExecutor}
                     .createStorageHelperWithOverride(
                         args, overrideParams, m_executionContext);
#endif

#if WITH_WEBDAV
    if (name == WEBDAV_HELPER_NAME)
        helper = WebDAVHelperFactory{m_webDAVExecutor}
                     .createStorageHelperWithOverride(
                         args, overrideParams, m_executionContext);

    if (name == HTTP_HELPER_NAME)
        helper =
            HTTPHelperFactory{m_webDAVExecutor}.createStorageHelperWithOverride(
                args, overrideParams, m_executionContext);
#endif

#if WITH_XROOTD
    if (name == XROOTD_HELPER_NAME)
        helper = XRootDHelperFactory{m_xrootdExecutor}
                     .createStorageHelperWithOverride(
                         args, overrideParams, m_executionContext);
#endif

#if WITH_NFS
    if (name == NFS_HELPER_NAME) {
        auto host = getParam<folly::fbstring>(args, "host");
        auto volume = getParam<folly::fbstring>(args, "volume");
        if (volume.find(';') == std::string::npos) {
            helper =
                NFSHelperFactory{m_nfsExecutor}.createStorageHelperWithOverride(
                    args, overrideParams, m_executionContext);
        }
        else {
            // This is a multisupport NFS helper which handles multiple NFS
            // volumes in parallel
            std::vector<StorageHelperPtr> storages =
                splitMultipleSupportNFSStorages(overrideParams, host, volume);

            helper = std::make_shared<StorageFanInHelper>(
                std::move(storages), m_nfsExecutor, m_executionContext);
        }
    }
#endif

    if (name == NULL_DEVICE_HELPER_NAME)
        helper = NullDeviceHelperFactory{m_nullDeviceExecutor}
                     .createStorageHelperWithOverride(
                         args, overrideParams, m_executionContext);

    if (!helper) {
        LOG(ERROR) << "Invalid storage helper name: " << name.toStdString();
        throw std::system_error{
            std::make_error_code(std::errc::invalid_argument),
            "Invalid storage helper name: '" + name.toStdString() + "'"};
    }

#ifdef WITH_BUFFERING
    if (buffered
#if WITH_WEBDAV
        && !(name == WEBDAV_HELPER_NAME)
#endif // WITH_WEBDAV
    ) {
        LOG_DBG(1) << "Created buffered helper of type: " << name;

#ifdef BUILD_PROXY_IO
        if (name == PROXY_HELPER_NAME) {
            // For proxy helper, limit the maximum read/write buffer size
            // to set an upper bound for Protobuf message
            auto proxyBufferLimits = m_bufferLimits;
            proxyBufferLimits.readBufferMaxSize =
                std::min(kProxyHelperMaximumReadBufferSize,
                    proxyBufferLimits.readBufferMaxSize);
            proxyBufferLimits.writeBufferMaxSize =
                std::min(kProxyHelperMaximumWriteBufferSize,
                    proxyBufferLimits.writeBufferMaxSize);

            // Make sure minimum buffer sizes aren't larger than maximum sizes
            proxyBufferLimits.readBufferMinSize =
                std::min(proxyBufferLimits.readBufferMinSize,
                    proxyBufferLimits.readBufferMaxSize);
            proxyBufferLimits.writeBufferMinSize =
                std::min(proxyBufferLimits.writeBufferMinSize,
                    proxyBufferLimits.writeBufferMaxSize);

            return std::make_shared<buffering::BufferAgent>(proxyBufferLimits,
                std::move(helper), m_scheduler, m_bufferMemoryLimitGuard);
        }
#endif // BUILD_PROXY_IO
        return std::make_shared<buffering::BufferAgent>(m_bufferLimits,
            std::move(helper), m_scheduler, m_bufferMemoryLimitGuard);
    }
#endif // WITH_BUFFERING

    LOG_DBG(1) << "Created non-buffered helper of type: " << name;

    return helper;
}

template <typename CommunicatorT>
std::vector<StorageHelperPtr>
StorageHelperCreator<CommunicatorT>::splitMultipleSupportNFSStorages(
    const std::unordered_map<folly::fbstring, folly::fbstring> &overrideParams,
    folly::fbstring &host, folly::fbstring &volume) const
{
    std::vector<StorageHelperPtr> storages;

    if (overrideParams.find("host") != overrideParams.cend())
        host = one::helpers::getParam<folly::fbstring>(overrideParams, "host");

    if (overrideParams.find("volume") != overrideParams.cend())
        volume =
            one::helpers::getParam<folly::fbstring>(overrideParams, "volume");

    std::vector<std::string> hosts;
    std::vector<std::string> volumes;
    folly::split(";", host, hosts, true);
    folly::split(";", volume, volumes, true);

    if (hosts.empty())
        throw std::system_error{
            std::make_error_code(std::errc::invalid_argument),
            "Invalid NFS host specification"};

    if (volumes.empty())
        throw std::system_error{
            std::make_error_code(std::errc::invalid_argument),
            "Invalid NFS volume specification"};

    if ((hosts.size() > 1) && (hosts.size() != volumes.size())) {
        throw std::system_error{
            std::make_error_code(std::errc::invalid_argument),
            "NFS host count must match NFS volume count in "
            "multisupport NFS helper"};
    }

    auto factory = one::helpers::NFSHelperFactory{this->m_nfsExecutor};
    for (auto i = 0U; i < volumes.size(); i++) {
        one::helpers::Params argsCopy;
        if (hosts.size() == 1)
            argsCopy["host"] = hosts[0];
        else
            argsCopy["host"] = hosts[i];

        argsCopy["volume"] = volumes[i];

        storages.emplace_back(factory.createStorageHelperWithOverride(
            argsCopy, overrideParams, this->m_executionContext));
    }

    return storages;
}

} // namespace helpers
} // namespace one

#endif // HELPERS_STORAGE_HELPER_FACTORY_H
