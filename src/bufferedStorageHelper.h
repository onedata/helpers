/**
 * @file bufferedStorageHelper.h
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

constexpr auto kDefaultBufferedStorageBufferSize =
    10 * 1024 * 1024 * 1024 * 1024;

class BufferedStorageFileHandle
    : public FileHandle,
      public std::enable_shared_from_this<BufferedStorageFileHandle> {
public:
    BufferedStorageFileHandle(
        folly::fbstring fileId, std::shared_ptr<BufferedStorageHelper> helper);

    virtual ~BufferedStorageFileHandle() = default;

    virtual folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    virtual folly::Future<std::size_t> write(const off_t offset,
        folly::IOBufQueue buf, WriteCallback &&writeCb) override;

    virtual folly::Future<folly::Unit> release() override;

    virtual folly::Future<folly::Unit> flush() override;

    virtual folly::Future<folly::Unit> fsync(bool isDataSync) override;

private:
    StorageHelperPtr m_buffer;
    StorageHelperPtr m_main;
};

class BufferedStorageHelper
    : public StorageHelper,
      public std::enable_shared_from_this<BufferedStorageHelper> {
public:
    /**
     * Constructor.
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    BufferedStorageHelper(StorageHelperPtr bufferStorage,
        StorageHelperPtr mainStorage,
        const std::size_t bufferStorageSize = kDefaultBufferedStorageBufferSize,
        folly::fbstring cachePrefix = {},
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    /**
     * Destructor.
     * Closes connection to StorageRouter storage cluster and destroys internal
     * context object.
     */
    virtual ~BufferedStorageHelper() = default;

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

    StorageHelperPtr bufferHelper() { return m_bufferStorage; };
    StorageHelperPtr mainHelper() { return m_mainStorage; };

private:
    StorageHelperPtr m_bufferStorage;
    StorageHelperPtr m_mainStorage;
};

/**
 * An implementation of @c StorageHelperFactory for StorageRouter storage
 * helper.
 */
class BufferedStorageHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    BufferedStorageHelperFactory(StorageHelperResolver &storageResolver)
        : m_storageResolver{storageResolver}
    {
        LOG_FCALL();
    }

    virtual folly::fbstring name() const override
    {
        return BUFFERED_STORAGE_HELPER_NAME;
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

        return std::make_shared<BufferedStorageHelper>(
            std::move(helpers), executionContext);
    }

private:
    StorageHelperResolver &m_storageResolver;
};

} // namespace helpers
} // namespace one
