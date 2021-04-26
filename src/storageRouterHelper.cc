/**
 * @file storageRouterHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "storageRouterHelper.h"

namespace one {
namespace helpers {

StorageRouterHelper::StorageRouterHelper(
    std::map<folly::fbstring, StorageHelperPtr> routes,
    ExecutionContext executionContext)
    : m_routes{routes.begin(), routes.end()}
{
    std::sort(
        m_routes.begin(), m_routes.end(), [](const auto &a, const auto &b) {
            return a.first.size() > b.first.size();
        });
}

StorageRouterHelper::~StorageRouterHelper() {}

StorageHelperPtr StorageRouterHelper::route(const folly::fbstring &fileId)
{
    for (const auto &route : m_routes) {
        if (fileId.find(route.first) != std::string::npos)
            return route.second;
    }

    throw makePosixException(EEXIST);
}

folly::fbstring StorageRouterHelper::name() const
{
    return STORAGE_ROUTER_HELPER_NAME;
}

folly::Future<struct stat> StorageRouterHelper::getattr(
    const folly::fbstring &fileId)
{
    return route(fileId)->getattr(fileId);
}

folly::Future<folly::Unit> StorageRouterHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    return route(fileId)->access(fileId, mask);
}

folly::Future<folly::fbstring> StorageRouterHelper::readlink(
    const folly::fbstring &fileId)
{
    return route(fileId)->readlink(fileId);
}

folly::Future<folly::fbvector<folly::fbstring>> StorageRouterHelper::readdir(
    const folly::fbstring &fileId, const off_t offset, const std::size_t count)
{
    return route(fileId)->readdir(fileId, offset, count);
}

folly::Future<folly::Unit> StorageRouterHelper::mknod(
    const folly::fbstring &fileId, const mode_t mode, const FlagsSet &flags,
    const dev_t rdev)
{
    return route(fileId)->mknod(fileId, mode, flags, rdev);
}

folly::Future<folly::Unit> StorageRouterHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    return route(fileId)->mkdir(fileId, mode);
}

folly::Future<folly::Unit> StorageRouterHelper::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    return route(fileId)->unlink(fileId, currentSize);
}

folly::Future<folly::Unit> StorageRouterHelper::rmdir(
    const folly::fbstring &fileId)
{
    return route(fileId)->rmdir(fileId);
}

folly::Future<folly::Unit> StorageRouterHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return route(to)->symlink(from, to);
}

folly::Future<folly::Unit> StorageRouterHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return route(to)->rename(from, to);
}

folly::Future<folly::Unit> StorageRouterHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return route(to)->link(from, to);
}

folly::Future<folly::Unit> StorageRouterHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    return route(fileId)->chmod(fileId, mode);
}

folly::Future<folly::Unit> StorageRouterHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    return route(fileId)->chown(fileId, uid, gid);
}

folly::Future<folly::Unit> StorageRouterHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    return route(fileId)->chown(fileId, size, currentSize);
}

folly::Future<FileHandlePtr> StorageRouterHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    return route(fileId)->open(fileId, flags, openParams);
}

folly::Future<ListObjectsResult> StorageRouterHelper::listobjects(
    const folly::fbstring &prefix, const folly::fbstring &marker,
    const off_t offset, const size_t count)
{
    // TODO handle case when prefix matches multiple routes
    return route(prefix)->listobjects(prefix, marker, offset, count);
}

folly::Future<folly::Unit> StorageRouterHelper::multipartCopy(
    const folly::fbstring &sourceKey, const folly::fbstring &destinationKey)
{
    throw std::system_error{
        std::make_error_code(std::errc::function_not_supported)};
}

folly::Future<folly::fbstring> StorageRouterHelper::getxattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    return route(uuid)->getxattr(uuid, name);
}

folly::Future<folly::Unit> StorageRouterHelper::setxattr(
    const folly::fbstring &uuid, const folly::fbstring &name,
    const folly::fbstring &value, bool create, bool replace)
{
    return route(uuid)->setxattr(uuid, name, value, create, replace);
}

folly::Future<folly::Unit> StorageRouterHelper::removexattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    return route(uuid)->removexattr(uuid, name);
}

folly::Future<folly::fbvector<folly::fbstring>> StorageRouterHelper::listxattr(
    const folly::fbstring &uuid)
{
    return route(uuid)->listxattr(uuid);
}
/*
StorageRouterFileHandle::StorageRouterFileHandle(
    folly::fbstring fileId, std::shared_ptr<StorageRouterHelper> helper)
{
}

folly::Future<folly::IOBufQueue> StorageRouterFileHandle::read(
    const off_t offset, const std::size_t size)
{
    throw std::system_error{
        std::make_error_code(std::errc::function_not_supported)};
}

folly::Future<std::size_t> StorageRouterFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    throw std::system_error{
        std::make_error_code(std::errc::function_not_supported)};
}

const Timeout &StorageRouterFileHandle::timeout() {}
*/
}
}

