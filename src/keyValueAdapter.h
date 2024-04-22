/**
 * @file keyValueAdapter.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_KEY_VALUE_ADAPTER_H
#define HELPERS_KEY_VALUE_ADAPTER_H

#include "helpers/storageHelper.h"

#include "keyValueAdapterParams.h"

#include <folly/Executor.h>
#include <folly/Hash.h>
#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace one {
namespace helpers {

template <typename T> struct StdHashCompare {
    bool equal(const T &a, const T &b) const { return a == b; }
    std::size_t hash(const T &a) const { return std::hash<T>()(a); }
};

class KeyValueAdapter;
class KeyValueHelper;

/**
 * The @c FileHandle implementation for key-value storage helpers.
 */
class KeyValueFileHandle
    : public FileHandle,
      public std::enable_shared_from_this<KeyValueFileHandle> {

    using Locks = tbb::concurrent_hash_map<folly::fbstring, bool,
        StdHashCompare<folly::fbstring>>;

public:
    /**
     * Constructor.
     * @param fileId Helper-specific ID of the open file.
     * @param helper Pointer to KeyValueAdapter instance.
     * @param blockSize Blocksize to use for read/write operations.
     * @param locks A structure for helper-wide locks of block ranges.
     * @param service @c io_service that will be used for some async operations.
     */
    KeyValueFileHandle(folly::fbstring fileId,
        std::shared_ptr<KeyValueAdapter> helper, const std::size_t blockSize,
        std::shared_ptr<Locks> locks,
        std::shared_ptr<folly::Executor> executor);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    const Timeout &timeout() override;

    bool isConcurrencyEnabled() const override { return true; }

private:
    folly::Future<std::size_t> writeFlat(const off_t offset,
        folly::IOBufQueue buf, std::size_t storageBlockSize,
        WriteCallback &&writeCb);

    folly::Future<folly::IOBufQueue> readFlat(const off_t offset,
        const std::size_t size, const std::size_t storageBlockSize);

    folly::Future<std::size_t> writeCanonical(
        const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb);

    folly::Future<folly::IOBufQueue> readCanonical(
        const off_t offset, const std::size_t size);

    folly::Future<folly::IOBufQueue> readBlocks(const off_t offset,
        const std::size_t requestedSize, const std::size_t storageBlockSize);

    folly::IOBufQueue readBlock(const uint64_t blockId, const off_t blockOffset,
        const std::size_t size);

    void writeBlock(
        folly::IOBufQueue buf, const uint64_t blockId, const off_t blockOffset);

    const std::size_t m_blockSize;
    std::shared_ptr<Locks> m_locks;
    std::shared_ptr<folly::Executor> m_executor;
};

/**
 * The @c KeyValueAdapter class translates POSIX operations to operations
 * available on key-value storage by splitting consistent range of bytes into
 * blocks.
 */
class KeyValueAdapter : public StorageHelper,
                        public std::enable_shared_from_this<KeyValueAdapter> {
    using Locks = tbb::concurrent_hash_map<folly::fbstring, bool,
        StdHashCompare<folly::fbstring>>;

public:
    using params_type = KeyValueAdapterParams;

    /**
     * Constructor.
     * @param helper @c KeyValueHelper instance that provides low level storage
     * access.
     * @param storagePathType Type of storage path mapping
     * @param blockSize Size of storage block.
     * @param randomAccess Specifies whether the underlying object storage
     *                     provides random access read/write functionality.
     */
    KeyValueAdapter(std::shared_ptr<KeyValueHelper> helper,
        std::shared_ptr<KeyValueAdapterParams> params,
        std::shared_ptr<folly::Executor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    KeyValueAdapter(const KeyValueAdapter &) = delete;
    KeyValueAdapter &operator=(const KeyValueAdapter &) = delete;
    KeyValueAdapter(KeyValueAdapter &&) = delete;
    KeyValueAdapter &operator=(KeyValueAdapter &&) = delete;

    virtual ~KeyValueAdapter() = default;

    folly::fbstring name() const override;

    HELPER_PARAM_GETTER(maxCanonicalObjectSize)

    folly::Future<folly::Unit> checkStorageAvailability() override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override;

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> multipartCopy(const folly::fbstring &sourceKey,
        const folly::fbstring &destinationKey, const std::size_t blockSize,
        const std::size_t size) override;

    folly::Future<ListObjectsResult> listobjects(const folly::fbstring &prefix,
        const folly::fbstring &marker, const off_t offset,
        const size_t count) override;

    virtual folly::Future<folly::Unit> fillMissingFileBlocks(
        const folly::fbstring &fileId, std::size_t size);

    bool isObjectStorage() const override { return true; }

    std::size_t blockSize() const noexcept override;

    std::shared_ptr<folly::Executor> executor() override { return m_executor; };

    std::shared_ptr<KeyValueHelper> helper() const { return m_helper; };

    std::vector<folly::fbstring> handleOverridableParams() const override;

private:
    std::shared_ptr<KeyValueHelper> m_helper;
    std::shared_ptr<folly::Executor> m_executor;
    std::shared_ptr<Locks> m_locks;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_KEY_VALUE_ADAPTER_H
