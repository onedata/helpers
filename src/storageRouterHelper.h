/**
 * @file storageRouterHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include "asioExecutor.h"
#include "helpers/logging.h"

namespace one {
namespace helpers {

namespace detail {
template <typename T> struct StringLengthCmp {
    bool operator()(const T &a, const T &b)
    {
        return a.first.size() > b.first.size();
    }
};
} // namespace detail

class StorageRouterHelper
    : public StorageHelper,
      public std::enable_shared_from_this<StorageRouterHelper> {
public:
    /**
     * Constructor.
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    StorageRouterHelper(std::map<folly::fbstring, StorageHelperPtr> routes,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    /**
     * Destructor.
     * Closes connection to StorageRouter storage cluster and destroys internal
     * context object.
     */
    virtual ~StorageRouterHelper();

    virtual folly::fbstring name() const override;

    virtual folly::Future<struct stat> getattr(const folly::fbstring &fileId);

    virtual folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask);

    virtual folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId);

    virtual folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count);

    virtual folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev);

    virtual folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode);

    virtual folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize);

    virtual folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId);

    virtual folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to);

    virtual folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to);

    virtual folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to);

    virtual folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode);

    virtual folly::Future<folly::Unit> chown(
        const folly::fbstring &fileId, const uid_t uid, const gid_t gid);

    virtual folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize);

    virtual folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams);

    virtual folly::Future<ListObjectsResult> listobjects(
        const folly::fbstring &prefix, const folly::fbstring &marker,
        const off_t offset, const size_t count);

    virtual folly::Future<folly::Unit> multipartCopy(
        const folly::fbstring &sourceKey,
        const folly::fbstring &destinationKey);

    virtual folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name);

    virtual folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace);

    virtual folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name);

    virtual folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid);

    virtual StorageHelperPtr route(const folly::fbstring &fileId);

private:
    // std::map<folly::fbstring, StorageHelperPtr, detail::StringLengthCmp>
    std::vector<std::pair<folly::fbstring, StorageHelperPtr>> m_routes;
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
    StorageRouterHelperFactory(StorageHelperResolver &storageResolver)
        : m_storageResolver{storageResolver}
    {
        LOG_FCALL();
    }

    virtual folly::fbstring name() const override
    {
        return STORAGE_ROUTER_HELPER_NAME;
    }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER)
    {
        std::map<folly::fbstring, StorageHelperPtr> helpers;
        // helpers.emplace();

        return std::make_shared<StorageRouterHelper>(
            std::move(helpers), executionContext);
    }

private:
    StorageHelperResolver &m_storageResolver;
};

} // namespace helpers
} // namespace one
