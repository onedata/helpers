/**
 * @file storageHelperCreator.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/storageHelperCreator.h"

#include "buffering/bufferAgent.h"
#include "logging.h"
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
#if WITH_DHUSODATA
#include "dhusodataHelper.h"
#endif

namespace one {
namespace helpers {

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
#if WITH_DHUSODATA
    asio::io_service &dhusodataService,
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
#if WITH_DHUSODATA
    m_dhusodataService(dhusodataService)
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
#if WITH_DHUSODATA
    asio::io_service &dhusodataService,
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
#if WITH_DHUSODATA
    m_dhusodataService(dhusodataService)
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
    const bool buffered)
{
    LOG_FCALL() << LOG_FARG(name) << LOG_FARGM(args) << LOG_FARG(buffered);

    StorageHelperPtr helper;

    if (name == POSIX_HELPER_NAME)
        helper = PosixHelperFactory{m_dioService}.createStorageHelper(args);

#if WITH_CEPH
    if (name == CEPH_HELPER_NAME)
        helper = CephHelperFactory{m_cephService}.createStorageHelper(args);

    if (name == CEPHRADOS_HELPER_NAME)
        helper = CephRadosHelperFactory{m_cephRadosService}.createStorageHelper(
            args);
#endif

#ifdef BUILD_PROXY_IO
    if (name == PROXY_HELPER_NAME)
        helper = ProxyHelperFactory{m_communicator}.createStorageHelper(args);
#endif

#if WITH_S3
    if (name == S3_HELPER_NAME)
        helper = S3HelperFactory{m_s3Service}.createStorageHelper(args);
#endif

#if WITH_SWIFT
    if (name == SWIFT_HELPER_NAME)
        helper = SwiftHelperFactory{m_swiftService}.createStorageHelper(args);
#endif

#if WITH_GLUSTERFS
    if (name == GLUSTERFS_HELPER_NAME)
        helper = GlusterFSHelperFactory{m_glusterfsService}.createStorageHelper(
            args);
#endif

#if WITH_WEBDAV
    if (name == WEBDAV_HELPER_NAME)
        helper =
            WebDAVHelperFactory{m_webDAVExecutor}.createStorageHelper(args);
#endif
#if WITH_DHUSODATA
    if (name == DHUSODATA_HELPER_NAME)
        helper = DHUSODataHelperFactory{m_dhusodataService}.createStorageHelper(
            args);
#endif

    if (name == NULL_DEVICE_HELPER_NAME)
        helper =
            NullDeviceHelperFactory{m_nullDeviceService}.createStorageHelper(
                args);

    if (!helper) {
        LOG(ERROR) << "Invalid storage helper name: " << name.toStdString();
        throw std::system_error{
            std::make_error_code(std::errc::invalid_argument),
            "Invalid storage helper name: '" + name.toStdString() + "'"};
    }

    if (buffered) {
        LOG_DBG(1) << "Created buffered helper of type: " << name;
        return std::make_shared<buffering::BufferAgent>(
            m_bufferLimits, helper, *m_scheduler, m_bufferMemoryLimitGuard);
    }

    LOG_DBG(1) << "Created non-buffered helper of type: " << name;

    return helper;
}

} // namespace helpers
} // namespace one
