/**
 * @file storageRouterHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include "helpers/logging.h"

namespace one {
namespace helpers {

class StorageRouterHelper
    : public StorageHelper,
      public std::enable_shared_from_this<StorageRouterHelper> {
public:
    /**
     * Constructor.
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    explicit StorageRouterHelper(
        std::map<folly::fbstring, StorageHelperPtr> routes,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    StorageRouterHelper(const StorageRouterHelper &) = delete;
    StorageRouterHelper &operator=(const StorageRouterHelper &) = delete;
    StorageRouterHelper(StorageRouterHelper &&) = delete;
    StorageRouterHelper &operator=(StorageRouterHelper &&) = delete;

    /**
     * Destructor.
     * Closes connection to StorageRouter storage cluster and destroys internal
     * context object.
     */
    ~StorageRouterHelper() override = default;

    folly::fbstring name() const override;

    folly::Future<folly::Unit> checkStorageAvailability() override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override;

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override;

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> chown(const folly::fbstring &fileId,
        const uid_t uid, const gid_t gid) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    folly::Future<ListObjectsResult> listobjects(const folly::fbstring &prefix,
        const folly::fbstring &marker, const off_t offset,
        const size_t count) override;

    folly::Future<folly::Unit> multipartCopy(const folly::fbstring &sourceKey,
        const folly::fbstring &destinationKey, const std::size_t blockSize,
        const std::size_t size) override;

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid) override;

    folly::Future<folly::Unit> loadBuffer(
        const folly::fbstring &uuid, const std::size_t size) override;

    folly::Future<folly::Unit> flushBuffer(
        const folly::fbstring &uuid, const std::size_t size) override;

    folly::Future<std::size_t> blockSizeForPath(
        const folly::fbstring &uuid) override;

    bool isObjectStorage() const override;

    virtual StorageHelperPtr route(const folly::fbstring &fileId);

    virtual folly::fbstring routePath(const folly::fbstring &fileId);

private:
    static folly::fbstring routeRelative(const StorageHelperPtr &helper,
        const folly::fbstring &route, const folly::fbstring &fileId);

    std::map<folly::fbstring, StorageHelperPtr> m_routes;
    std::vector<folly::fbstring> m_routesOrder;
};

/**
 * An implementation of @c StorageHelperFactory for StorageRouter storage
 * helper.
 */
class StorageRouterHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    StorageRouterHelperFactory() { LOG_FCALL(); }

    folly::fbstring name() const override { return STORAGE_ROUTER_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params & /*parameters*/,
        ExecutionContext executionContext) override
    {
        std::map<folly::fbstring, StorageHelperPtr> routes;
        return std::make_shared<StorageRouterHelper>(
            std::move(routes), executionContext);
    }
};

} // namespace helpers
} // namespace one
