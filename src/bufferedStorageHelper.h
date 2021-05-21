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

constexpr std::size_t kDefaultBufferedStorageBufferSize =
    10 * 1024ULL * 1024ULL * 1024ULL * 1024ULL;

class BufferedStorageHelper;

class BufferedStorageFileHandle
    : public FileHandle,
      public std::enable_shared_from_this<BufferedStorageFileHandle> {
public:
    BufferedStorageFileHandle(folly::fbstring fileId,
        std::shared_ptr<BufferedStorageHelper> helper,
        FileHandlePtr bufferStorageHandle, FileHandlePtr mainStorageHandle);

    virtual ~BufferedStorageFileHandle() = default;

    virtual folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    virtual folly::Future<std::size_t> write(const off_t offset,
        folly::IOBufQueue buf, WriteCallback &&writeCb) override;

    virtual folly::Future<folly::Unit> release() override;

    virtual folly::Future<folly::Unit> flush() override;

    virtual folly::Future<folly::Unit> fsync(bool isDataSync) override;

    const Timeout &timeout() override { return m_mainStorageHandle->timeout(); }

    folly::Future<folly::Unit> loadBufferBlocks(
        const off_t offset, const std::size_t size);

private:
    StorageHelperPtr m_bufferStorageHelper;
    StorageHelperPtr m_mainStorageHelper;
    FileHandlePtr m_bufferStorageHandle;
    FileHandlePtr m_mainStorageHandle;
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
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER,
        folly::fbstring bufferPath = {}, const int bufferDepth = 1,
        const std::size_t bufferStorageSize =
            kDefaultBufferedStorageBufferSize);

    /**
     * Destructor.
     * Closes connection to StorageRouter storage cluster and destroys internal
     * context object.
     */
    virtual ~BufferedStorageHelper() = default;

    virtual folly::fbstring name() const override;

    virtual folly::Future<struct stat> getattr(
        const folly::fbstring &fileId) override;

    virtual folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    virtual folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override;

    virtual folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count) override;

    virtual folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override;

    virtual folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override;

    virtual folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    virtual folly::Future<folly::Unit> rmdir(
        const folly::fbstring &fileId) override;

    virtual folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to) override;

    virtual folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    virtual folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to) override;

    virtual folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override;

    virtual folly::Future<folly::Unit> chown(const folly::fbstring &fileId,
        const uid_t uid, const gid_t gid) override;

    virtual folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    virtual folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    virtual folly::Future<ListObjectsResult> listobjects(
        const folly::fbstring &prefix, const folly::fbstring &marker,
        const off_t offset, const size_t count) override;

    virtual folly::Future<folly::Unit> multipartCopy(
        const folly::fbstring &sourceKey, const folly::fbstring &destinationKey,
        const std::size_t blockSize, const std::size_t size) override;

    virtual folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    virtual folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    virtual folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    virtual folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid) override;

    virtual folly::Future<folly::Unit> loadBuffer(
        const folly::fbstring &fileId, const std::size_t size) override;

    virtual folly::Future<folly::Unit> flushBuffer(
        const folly::fbstring &fileId, const std::size_t size) override;

    StorageHelperPtr bufferHelper() { return m_bufferStorage; };
    StorageHelperPtr mainHelper() { return m_mainStorage; };

    folly::fbstring toBufferPath(const folly::fbstring &fileId);

private:
    /**
     * Applies the requested operations on both buffer and main storages
     * asynchronously and returns a pair of results
     */
    template <typename T, typename F>
    folly::Future<std::pair<T, T>> applyAsync(
        F &&bufferOp, F &&mainOp, const bool ignoreBufferError = false);

    StorageHelperPtr m_bufferStorage;
    StorageHelperPtr m_mainStorage;

    folly::fbstring m_bufferPath;
    int m_bufferDepth;
};

/**
 * An implementation of @c StorageHelperFactory for BufferedStorageHelper.
 */
class BufferedStorageHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    BufferedStorageHelperFactory() { LOG_FCALL(); }

    virtual folly::fbstring name() const override
    {
        return BUFFERED_STORAGE_HELPER_NAME;
    }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters,
        ExecutionContext executionContext =
            ExecutionContext::ONEPROVIDER) override
    {
        return {};
    }

    std::shared_ptr<BufferedStorageHelper> createStorageHelper(
        StorageHelperPtr bufferStorageHelper,
        StorageHelperPtr mainStorageHelper, const Params &parameters,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER)
    {
        return std::make_shared<BufferedStorageHelper>(
            std::move(bufferStorageHelper), std::move(mainStorageHelper),
            executionContext,
            getParam<folly::fbstring>(
                parameters, "bufferPath", ".__onedata__buffer"),
            getParam<int>(parameters, "bufferDepth", 1),
            kDefaultBufferedStorageBufferSize);
    }
};

} // namespace helpers
} // namespace one
