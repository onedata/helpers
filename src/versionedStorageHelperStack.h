/**
 * @file versionedStorageHelperStack.h
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include <deque>

namespace one {
namespace helpers {

class VersionedStorageHelper : public StorageHelper {
public:
    template <typename T>
    static std::shared_ptr<VersionedStorageHelper> create(
        std::shared_ptr<StorageHelperParams> params,
        std::shared_ptr<folly::Executor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER)
    {
        auto helper = std::make_shared<T>(
            std::dynamic_pointer_cast<typename T::params_type>(params),
            executor, executionContext);

        return std::make_shared<VersionedStorageHelper>(std::move(helper));
    }

    VersionedStorageHelper(StorageHelperPtr helper)
    {
        m_helpers.push_front(std::move(helper));
    }

    folly::fbstring name() const override { return getHelper()->name(); }

    folly::Future<struct stat> getattr(const folly::fbstring &fileId)
    {
        return getHelper()->getattr(fileId);
    }

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, int mask) override
    {
        return getHelper()->access(fileId, mask);
    }

    folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override
    {
        return getHelper()->readlink(fileId);
    }

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, std::size_t count) override
    {
        return getHelper()->readdir(fileId, offset, count);
    }

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId, mode_t mode,
        const FlagsSet &flags, dev_t rdev) override
    {
        return getHelper()->mknod(fileId, mode, flags, rdev);
    }

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, mode_t mode) override
    {
        return getHelper()->mkdir(fileId, mode);
    }

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, size_t currentSize) override
    {
        return getHelper()->unlink(fileId, currentSize);
    }

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override
    {
        return getHelper()->rmdir(fileId);
    }

    folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to) override
    {
        return getHelper()->symlink(from, to);
    }

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override
    {
        return getHelper()->rename(from, to);
    }

    folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to) override
    {
        return getHelper()->link(from, to);
    }

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, mode_t mode) override
    {
        return getHelper()->chmod(fileId, mode);
    }

    folly::Future<folly::Unit> chown(
        const folly::fbstring &fileId, uid_t uid, gid_t gid) override
    {
        return getHelper()->chown(fileId, uid, gid);
    }

    folly::Future<folly::Unit> truncate(
        const folly::fbstring &fileId, off_t size, size_t currentSize) override
    {
        return getHelper()->truncate(fileId, size, currentSize);
    }

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const FlagsSet &flags, const Params &openParams) override
    {
        return getHelper()->open(fileId, flags, openParams);
    }

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const FlagsSet &flags, const Params &openParams,
        const Params &helperOverrideParams) override
    {
        return getHelper()->open(
            fileId, flags, openParams, helperOverrideParams);
    }

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId, int flags,
        const Params &openParams) override
    {
        return getHelper()->open(fileId, flags, openParams);
    }

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId, int flags,
        const Params &openParams, const Params &helperOverrideParams) override
    {
        return getHelper()->open(
            fileId, flags, openParams, helperOverrideParams);
    }

    folly::Future<ListObjectsResult> listobjects(const folly::fbstring &prefix,
        const folly::fbstring &marker, off_t offset, size_t count) override
    {
        return getHelper()->listobjects(prefix, marker, offset, count);
    }

    folly::Future<folly::Unit> multipartCopy(const folly::fbstring &sourceKey,
        const folly::fbstring &destinationKey, std::size_t blockSize,
        std::size_t size) override
    {
        return getHelper()->multipartCopy(
            sourceKey, destinationKey, blockSize, size);
    }

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override
    {
        return getHelper()->getxattr(uuid, name);
    }

    folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override
    {
        return getHelper()->setxattr(uuid, name, value, create, replace);
    }

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override
    {
        return getHelper()->removexattr(uuid, name);
    }

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid) override
    {
        return getHelper()->listxattr(uuid);
    }

    folly::Future<folly::Unit> loadBuffer(
        const folly::fbstring &fileId, std::size_t size) override
    {
        return getHelper()->loadBuffer(fileId, size);
    }

    folly::Future<folly::Unit> flushBuffer(
        const folly::fbstring &fileId, std::size_t size) override
    {
        return getHelper()->flushBuffer(fileId, size);
    }

    folly::Future<std::size_t> blockSizeForPath(
        const folly::fbstring &fileId) override
    {
        return getHelper()->blockSizeForPath(fileId);
    }

    folly::Future<folly::Unit> refreshParams(
        std::shared_ptr<StorageHelperParams> params) override
    {
        //        auto newHelper = create(params);
    }

    StorageHelperPtr getHelper() const
    {
        std::lock_guard<std::mutex> lock{m_helpersMutex};

        if (m_helpers.empty())
            return {};

        return m_helpers.front();
    }

private:
    mutable std::mutex m_helpersMutex;
    std::deque<StorageHelperPtr> m_helpers;
};

} // namespace helpers
} // namespace one
