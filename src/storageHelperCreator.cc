/**
 * @file storageHelperCreator.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/storageHelperCreator.h"

#include "buffering/bufferAgent.h"
#include "helpers/logging.h"
#include "nullDeviceHelper.h"
#include "posixHelper.h"
#include "proxyHelper.h"
#include "scheduler.h"

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
#include "webDAVHelper.h"
#include <folly/executors/IOExecutor.h>
#endif

namespace one {
namespace helpers {

constexpr std::size_t kProxyHelperMaximumReadBufferSize = 52'428'800;
constexpr std::size_t kProxyHelperMaximumWriteBufferSize = 52'428'800;

#ifdef BUILD_PROXY_IO

StorageHelperCreator::StorageHelperCreator(
#if WITH_CEPH
    asio::io_service &cephService, asio::io_service &cephRadosService,
#endif
    asio::io_service &dioService,
#if WITH_S3
    asio::io_service &s3Service,
#endif
#if WITH_SWIFT
    asio::io_service &swiftService,
#endif
#if WITH_GLUSTERFS
    asio::io_service &glusterfsService,
#endif
#if WITH_WEBDAV
    std::shared_ptr<folly::IOExecutor> webDAVExecutor,
#endif
    asio::io_service &nullDeviceService,
    communication::Communicator &communicator,
    std::size_t bufferSchedulerWorkers, buffering::BufferLimits bufferLimits)
    :
#if WITH_CEPH
    m_cephService{cephService}
    , m_cephRadosService{cephRadosService}
    ,
#endif
    m_dioService{dioService}
    ,
#if WITH_S3
    m_s3Service{s3Service}
    ,
#endif
#if WITH_SWIFT
    m_swiftService{swiftService}
    ,
#endif
#if WITH_GLUSTERFS
    m_glusterfsService{glusterfsService}
    ,
#endif
#if WITH_WEBDAV
    m_webDAVExecutor{std::move(webDAVExecutor)}
    ,
#endif
    m_nullDeviceService{nullDeviceService}
    , m_scheduler{std::make_unique<Scheduler>(bufferSchedulerWorkers)}
    , m_bufferLimits{bufferLimits}
    , m_bufferMemoryLimitGuard{std::make_shared<
          buffering::BufferAgentsMemoryLimitGuard>(bufferLimits)}
    , m_communicator{communicator}
{
}
#else

StorageHelperCreator::StorageHelperCreator(
#if WITH_CEPH
    asio::io_service &cephService, asio::io_service &cephRadosService,
#endif
    asio::io_service &dioService,
#if WITH_S3
    asio::io_service &s3Service,
#endif
#if WITH_SWIFT
    asio::io_service &swiftService,
#endif
#if WITH_GLUSTERFS
    asio::io_service &glusterfsService,
#endif
#if WITH_WEBDAV
    std::shared_ptr<folly::IOExecutor> webDAVExecutor,
#endif
    asio::io_service &nullDeviceService, std::size_t bufferSchedulerWorkers,
    buffering::BufferLimits bufferLimits)
    :
#if WITH_CEPH
    m_cephService{cephService}
    , m_cephRadosService{cephRadosService}
    ,
#endif
    m_dioService{dioService}
    ,
#if WITH_S3
    m_s3Service{s3Service}
    ,
#endif
#if WITH_SWIFT
    m_swiftService{swiftService}
    ,
#endif
#if WITH_GLUSTERFS
    m_glusterfsService{glusterfsService}
    ,
#endif
#if WITH_WEBDAV
    m_webDAVExecutor{std::move(webDAVExecutor)}
    ,
#endif
    m_nullDeviceService{nullDeviceService}
    , m_scheduler{std::make_unique<Scheduler>(bufferSchedulerWorkers)}
    , m_bufferLimits{bufferLimits}
    , m_bufferMemoryLimitGuard{
          std::make_shared<buffering::BufferAgentsMemoryLimitGuard>(
              bufferLimits)}
{
}
#endif

StorageHelperCreator::~StorageHelperCreator() = default;
std::shared_ptr<StorageHelper> StorageHelperCreator::getStorageHelper(
    const folly::fbstring &name,
    const std::unordered_map<folly::fbstring, folly::fbstring> &args,
    const bool buffered,
    const std::unordered_map<folly::fbstring, folly::fbstring> &overrideParams)
{
    LOG_FCALL() << LOG_FARG(name) << LOG_FARGM(args) << LOG_FARG(buffered);

    StorageHelperPtr helper;

    if (name == POSIX_HELPER_NAME) {
        helper =
            PosixHelperFactory{m_dioService}.createStorageHelperWithOverride(
                args, overrideParams);
    }

#if WITH_CEPH
    if (name == CEPH_HELPER_NAME)
        helper =
            CephHelperFactory{m_cephService}.createStorageHelperWithOverride(
                args, overrideParams);

    if (name == CEPHRADOS_HELPER_NAME)
        helper = CephRadosHelperFactory{m_cephRadosService}
                     .createStorageHelperWithOverride(args, overrideParams);
#endif

#ifdef BUILD_PROXY_IO
    if (name == PROXY_HELPER_NAME)
        helper =
            ProxyHelperFactory{m_communicator}.createStorageHelperWithOverride(
                args, overrideParams);
#endif

#if WITH_S3
    if (name == S3_HELPER_NAME)
        helper = S3HelperFactory{m_s3Service}.createStorageHelperWithOverride(
            args, overrideParams);
#endif

#if WITH_SWIFT
    if (name == SWIFT_HELPER_NAME)
        helper =
            SwiftHelperFactory{m_swiftService}.createStorageHelperWithOverride(
                args, overrideParams);
#endif

#if WITH_GLUSTERFS
    if (name == GLUSTERFS_HELPER_NAME)
        helper = GlusterFSHelperFactory{m_glusterfsService}
                     .createStorageHelperWithOverride(args, overrideParams);
#endif

#if WITH_WEBDAV
    if (name == WEBDAV_HELPER_NAME)
        helper = WebDAVHelperFactory{m_webDAVExecutor}
                     .createStorageHelperWithOverride(args, overrideParams);
#endif

    if (name == NULL_DEVICE_HELPER_NAME)
        helper = NullDeviceHelperFactory{m_nullDeviceService}
                     .createStorageHelperWithOverride(args, overrideParams);

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
                std::move(helper), *m_scheduler, m_bufferMemoryLimitGuard);
        }

        return std::make_shared<buffering::BufferAgent>(m_bufferLimits,
            std::move(helper), *m_scheduler, m_bufferMemoryLimitGuard);
    }

    LOG_DBG(1) << "Created non-buffered helper of type: " << name;

    return helper;
}

} // namespace helpers
} // namespace one
