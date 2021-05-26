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

// NOLINTNEXTLINE
#define ROUTE(__ROUTE__name, __ROUTE__fileId, ...)                             \
    do {                                                                       \
        auto rp = routePath(__ROUTE__fileId);                                  \
        auto rh = route(__ROUTE__fileId);                                      \
        return rh->__ROUTE__name(                                              \
            routeRelative(rh, rp, __ROUTE__fileId), __VA_ARGS__);              \
    } while (0);

// NOLINTNEXTLINE
#define ROUTE_NO_ARGS(__ROUTE__name, __ROUTE__fileId)                          \
    do {                                                                       \
        auto rp = routePath(__ROUTE__fileId);                                  \
        auto rh = route(__ROUTE__fileId);                                      \
        return rh->__ROUTE__name(routeRelative(rh, rp, __ROUTE__fileId));      \
    } while (0);

// NOLINTNEXTLINE
#define ROUTE_FROM_TO(__ROUTE__name, __ROUTE__from, __ROUTE__to)               \
    do {                                                                       \
        auto rfrom = routePath(__ROUTE__from);                                 \
        auto rto = routePath(__ROUTE__to);                                     \
        auto hfrom = route(__ROUTE__from);                                     \
        auto hto = route(__ROUTE__to);                                         \
        if (hto.get() != hfrom.get())                                          \
            throw makePosixException(ENOTSUP);                                 \
        return hfrom->__ROUTE__name(                                           \
            routeRelative(hfrom, rfrom, __ROUTE__from),                        \
            routeRelative(hto, rto, __ROUTE__to));                             \
    } while (0);

StorageRouterHelper::StorageRouterHelper(
    std::map<folly::fbstring, StorageHelperPtr> routes,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_routes{std::move(routes)}
{
    for (const auto &rs : m_routes) {
        m_routesOrder.push_back(rs.first);
    }

    std::sort(m_routesOrder.begin(), m_routesOrder.end(),
        [](const auto &a, const auto &b) { return a.size() > b.size(); });
}

folly::fbstring StorageRouterHelper::routePath(const folly::fbstring &fileId)
{
    if (fileId.empty())
        throw makePosixException(ENOENT);

    folly::fbstring fileIdRoute{fileId};
    if (fileId.front() == '/')
        fileIdRoute = fileId.substr(1);

    // Skip space id from routing
    fileIdRoute = fileIdRoute.substr(fileIdRoute.find_first_of('/'));

    for (const auto &route : m_routesOrder) {
        if (fileIdRoute.find(route) == 0) {
            return route;
        }
    }

    throw makePosixException(ENOENT);
}

StorageHelperPtr StorageRouterHelper::route(const folly::fbstring &fileId)
{
    return m_routes.at(routePath(fileId));
}

folly::fbstring StorageRouterHelper::routeRelative(StorageHelperPtr /*helper*/,
    const folly::fbstring & /*route*/, const folly::fbstring &fileId)
{
    return fileId;
}

folly::fbstring StorageRouterHelper::name() const
{
    return STORAGE_ROUTER_HELPER_NAME;
}

folly::Future<struct stat> StorageRouterHelper::getattr(
    const folly::fbstring &fileId)
{
    ROUTE_NO_ARGS(getattr, fileId);
}

folly::Future<folly::Unit> StorageRouterHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    ROUTE(access, fileId, mask);
}

folly::Future<folly::fbstring> StorageRouterHelper::readlink(
    const folly::fbstring &fileId)
{
    ROUTE_NO_ARGS(readlink, fileId);
}

folly::Future<folly::fbvector<folly::fbstring>> StorageRouterHelper::readdir(
    const folly::fbstring &fileId, const off_t offset, const std::size_t count)
{
    ROUTE(readdir, fileId, offset, count);
}

folly::Future<folly::Unit> StorageRouterHelper::mknod(
    const folly::fbstring &fileId, const mode_t mode, const FlagsSet &flags,
    const dev_t rdev)
{
    ROUTE(mknod, fileId, mode, flags, rdev);
}

folly::Future<folly::Unit> StorageRouterHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    ROUTE(mkdir, fileId, mode);
}

folly::Future<folly::Unit> StorageRouterHelper::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    ROUTE(unlink, fileId, currentSize);
}

folly::Future<folly::Unit> StorageRouterHelper::rmdir(
    const folly::fbstring &fileId)
{
    ROUTE_NO_ARGS(rmdir, fileId);
}

folly::Future<folly::Unit> StorageRouterHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    ROUTE_FROM_TO(symlink, from, to);
}

folly::Future<folly::Unit> StorageRouterHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    ROUTE_FROM_TO(rename, from, to);
}

folly::Future<folly::Unit> StorageRouterHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    ROUTE_FROM_TO(link, from, to);
}

folly::Future<folly::Unit> StorageRouterHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    ROUTE(chmod, fileId, mode);
}

folly::Future<folly::Unit> StorageRouterHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    ROUTE(chown, fileId, uid, gid);
}

folly::Future<folly::Unit> StorageRouterHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    ROUTE(truncate, fileId, size, currentSize);
}

folly::Future<FileHandlePtr> StorageRouterHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    ROUTE(open, fileId, flags, openParams);
}

folly::Future<ListObjectsResult> StorageRouterHelper::listobjects(
    const folly::fbstring &prefix, const folly::fbstring &marker,
    const off_t offset, const size_t count)
{
    ROUTE(listobjects, prefix, marker, offset, count);
}

folly::Future<folly::Unit> StorageRouterHelper::multipartCopy(
    const folly::fbstring & /*sourceKey*/,
    const folly::fbstring & /*destinationKey*/, const std::size_t /*size*/)
{
    throw std::system_error{
        std::make_error_code(std::errc::function_not_supported)};
}

folly::Future<folly::fbstring> StorageRouterHelper::getxattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    ROUTE(getxattr, uuid, name);
}

folly::Future<folly::Unit> StorageRouterHelper::setxattr(
    const folly::fbstring &uuid, const folly::fbstring &name,
    const folly::fbstring &value, bool create, bool replace)
{
    ROUTE(setxattr, uuid, name, value, create, replace);
}

folly::Future<folly::Unit> StorageRouterHelper::removexattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    ROUTE(removexattr, uuid, name);
}

folly::Future<folly::fbvector<folly::fbstring>> StorageRouterHelper::listxattr(
    const folly::fbstring &uuid)
{
    ROUTE_NO_ARGS(listxattr, uuid);
}

folly::Future<folly::Unit> StorageRouterHelper::loadBuffer(
    const folly::fbstring &uuid, const std::size_t size)
{
    ROUTE(loadBuffer, uuid, size);
}

folly::Future<folly::Unit> StorageRouterHelper::flushBuffer(
    const folly::fbstring &uuid, const std::size_t size)
{
    ROUTE(flushBuffer, uuid, size);
}

folly::Future<std::size_t> StorageRouterHelper::blockSizeForPath(
    const folly::fbstring &uuid)
{
    ROUTE_NO_ARGS(blockSizeForPath, uuid);
}

bool StorageRouterHelper::isObjectStorage() const
{
    // Return true if any of the storages is object storage
    for (auto &routes : m_routes)
        if (routes.second->isObjectStorage())
            return true;

    return false;
}
} // namespace helpers
} // namespace one
