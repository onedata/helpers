/**
 * @file storageHelperCreator.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/storageHelperCreator.h"

#include "bufferedStorageHelper.h"
#include "buffering/bufferAgent.h"
#include "helpers/logging.h"
#include "nullDeviceHelper.h"
#include "posixHelper.h"
#include "proxyHelper.h"
#include "scheduler.h"
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

namespace one {
namespace helpers {

constexpr std::size_t kProxyHelperMaximumReadBufferSize = 52'428'800;
constexpr std::size_t kProxyHelperMaximumWriteBufferSize = 52'428'800;

#ifdef BUILD_PROXY_IO

StorageHelperCreator::StorageHelperCreator(
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
    communication::Communicator &communicator,
    std::size_t bufferSchedulerWorkers, buffering::BufferLimits bufferLimits,
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
    m_nullDeviceExecutor{std::move(nullDeviceExecutor)}
    , m_scheduler{std::make_unique<Scheduler>(bufferSchedulerWorkers)}
    , m_bufferLimits{bufferLimits}
    , m_bufferMemoryLimitGuard{std::make_shared<
          buffering::BufferAgentsMemoryLimitGuard>(bufferLimits)}
    , m_communicator{communicator}
    , m_executionContext{executionContext}
{
}
#else

StorageHelperCreator::StorageHelperCreator(
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
    std::size_t bufferSchedulerWorkers, buffering::BufferLimits bufferLimits,
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
    m_nullDeviceExecutor{std::move(nullDeviceExecutor)}
    , m_scheduler{std::make_shared<Scheduler>(bufferSchedulerWorkers)}
    , m_bufferLimits{bufferLimits}
    , m_bufferMemoryLimitGuard{std::make_shared<
          buffering::BufferAgentsMemoryLimitGuard>(bufferLimits)}
    , m_executionContext{executionContext}
{
}
#endif

std::shared_ptr<StorageHelper> StorageHelperCreator::getStorageHelper(
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
        helper =
            PosixHelperFactory{m_dioExecutor}.createStorageHelperWithOverride(
                args, overrideParams, m_executionContext);
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
        helper =
            ProxyHelperFactory{m_communicator}.createStorageHelperWithOverride(
                args, overrideParams, m_executionContext);
#endif

#if WITH_S3
    if (name == S3_HELPER_NAME) {
        if (getParam<bool>(args, "archiveStorage", false)) {
            const auto kDefaultBufferStorageBlockSizeMultiplier = 5UL;
            auto bufferArgs{args};
            auto mainArgs{args};
            auto bufferedArgs{args};
            bufferArgs["blockSize"] = std::to_string(
                kDefaultBufferStorageBlockSizeMultiplier *
                getParam<std::size_t>(args, "blockSize", DEFAULT_BLOCK_SIZE));
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

    if (buffered
#if WITH_WEBDAV
        && !(name == WEBDAV_HELPER_NAME)
#endif
    ) {
        LOG_DBG(1) << "Created buffered helper of type: " << name;

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

        return std::make_shared<buffering::BufferAgent>(m_bufferLimits,
            std::move(helper), m_scheduler, m_bufferMemoryLimitGuard);
    }

    LOG_DBG(1) << "Created non-buffered helper of type: " << name;

    return helper;
}

} // namespace helpers
} // namespace one
