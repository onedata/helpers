/**
 * @file bufferAgent.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_BUFFERING_BUFFER_AGENT_H
#define HELPERS_BUFFERING_BUFFER_AGENT_H

#include "buffering/bufferLimits.h"
#include "helpers/logging.h"
#include "helpers/storageHelper.h"
#include "scheduler.h"

#include <chrono>
#include <memory>
#include <mutex>

namespace one {
namespace helpers {
namespace buffering {

class ReadCache;
class WriteBuffer;

/**
 * This class maintains a counter for global read and write helper buffers
 * allocations, to maintain a maximum overall buffers size.
 */
class BufferAgentsMemoryLimitGuard {
public:
    /**
     * Constructor
     * @param bufferLimits Reference to application buffer limits
     */
    explicit BufferAgentsMemoryLimitGuard(const BufferLimits &bufferLimits);

    /**
     * This function tries to mark readSize and writeSize bytes as reserved with
     * respect to total buffer limits specified in the application buffer
     * limits. No actual memory allocation is done here. If this function
     * returns false, it means that the global buffer memory limit has been
     * exhausted and buffered file handles cannot be created until some other
     * are closed.
     *
     * @param readSize The size in bytes of maximum memory needed by the read
     *                 buffer
     * @param writeSize The size in bytes of maximum memory needed by the write
     *                  buffer
     */
    bool reserveBuffers(size_t readSize, size_t writeSize);

    /**
     * This function tries to release the readSize and writeSize bytes from the
     * memory limit guard.
     *
     * @param readSize The size in bytes of maximum memory used by the read
     *                 buffer
     * @param writeSize The size in bytes of maximum memory used by the write
     *                  buffer
     */
    bool releaseBuffers(size_t readSize, size_t writeSize);

private:
    const BufferLimits m_bufferLimits;

    std::mutex m_mutex;
    size_t m_readBuffersReservedSize;
    size_t m_writeBuffersReservedSize;
};

class BufferAgent;

class BufferedFileHandle : public FileHandle {
public:
    BufferedFileHandle(const folly::fbstring &fileId,
        FileHandlePtr wrappedHandle, const BufferLimits &bl,
        std::shared_ptr<Scheduler> scheduler,
        std::shared_ptr<BufferAgent> bufferAgent,
        std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard);

    BufferedFileHandle(const BufferedFileHandle &) = delete;
    BufferedFileHandle &operator=(const BufferedFileHandle &) = delete;
    BufferedFileHandle(BufferedFileHandle &&) = default;
    BufferedFileHandle &operator=(BufferedFileHandle &&) = default;

    ~BufferedFileHandle() override;

    folly::Future<folly::IOBufQueue> read(
        off_t offset, std::size_t size) override;

    folly::Future<folly::IOBufQueue> readContinuous(
        off_t offset, std::size_t size, std::size_t continuousSize) override;

    folly::Future<std::size_t> write(
        off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb) override;

    folly::Future<folly::Unit> fsync(bool isDataSync) override;

    folly::Future<folly::Unit> flush() override;

    folly::Future<folly::Unit> release() override;

    const Timeout &timeout() override;

    bool needsDataConsistencyCheck() override;

    std::size_t wouldPrefetch(off_t offset, std::size_t size) override;

    folly::Future<folly::Unit> flushUnderlying() override;

    FileHandlePtr wrappedHandle();

    folly::Future<folly::Unit> refreshHelperParams(
        std::shared_ptr<StorageHelperParams> params) override;

private:
    FileHandlePtr m_wrappedHandle;
    BufferLimits m_bufferLimits;
    std::shared_ptr<ReadCache> m_readCache;
    std::shared_ptr<WriteBuffer> m_writeBuffer;
    std::shared_ptr<BufferAgentsMemoryLimitGuard> m_bufferMemoryLimitGuard;
    std::shared_ptr<Scheduler> m_scheduler;
};

class BufferAgent : public StorageHelper,
                    public std::enable_shared_from_this<BufferAgent> {
public:
    BufferAgent(BufferLimits bufferLimits, StorageHelperPtr helper,
        std::shared_ptr<Scheduler> scheduler,
        std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard);

    folly::fbstring name() const override;

    std::shared_ptr<folly::Executor> executor() override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &params) override;

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

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid) override;

    folly::Future<std::shared_ptr<StorageHelperParams>> params() const override;

    folly::Future<folly::Unit> refreshParams(
        std::shared_ptr<StorageHelperParams> params) override;

    bool isBuffered() const override;

    StorageHelperPtr helper();

    const Timeout &timeout() override;

private:
    BufferLimits m_bufferLimits;
    StorageHelperPtr m_helper;
    std::shared_ptr<Scheduler> m_scheduler;
    std::shared_ptr<BufferAgentsMemoryLimitGuard> m_bufferMemoryLimitGuard;
};

} // namespace buffering
} // namespace helpers
} // namespace one

#endif // HELPERS_BUFFERING_BUFFER_AGENT_H
