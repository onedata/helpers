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

#include <folly/Executor.h>
#include <folly/Hash.h>
#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace one {
namespace helpers {

constexpr std::size_t DEFAULT_BLOCK_SIZE = 10 * 1024 * 1024;

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

    virtual folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    virtual folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf) override;

    const Timeout &timeout() override;

    bool isConcurrencyEnabled() const override { return true; }

private:
    folly::Future<folly::IOBufQueue> readBlocks(
        const off_t offset, const std::size_t requestedSize);

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
    /**
     * Constructor.
     * @param helper @c KeyValueHelper instance that provides low level storage
     * access.
     * @param service IO service used for asynchronous operations.
     * @param locks Map of locks used to exclude concurrent operations on the
     * same storage block.
     * @param blockSize Size of storage block.
     * @param randomAccess Specifies whether the underlying object storage
     *                     provides random access read/write functionality.
     */
    KeyValueAdapter(std::shared_ptr<KeyValueHelper> helper,
        std::shared_ptr<folly::Executor> executor,
        std::size_t blockSize = DEFAULT_BLOCK_SIZE);

    virtual folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    virtual folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    virtual folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    virtual folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override
    {
        return folly::makeFuture();
    }

    virtual folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    virtual folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    const Timeout &timeout() override;

    std::shared_ptr<folly::Executor> executor() override { return m_executor; };

    std::shared_ptr<KeyValueHelper> helper() { return m_helper; };

private:
    std::shared_ptr<KeyValueHelper> m_helper;
    std::shared_ptr<folly::Executor> m_executor;
    std::shared_ptr<Locks> m_locks;
    const std::size_t m_blockSize;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_KEY_VALUE_ADAPTER_H
