/**
 * @file bufferAgent.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_BUFFERING_BUFFER_AGENT_H
#define HELPERS_BUFFERING_BUFFER_AGENT_H

#include "communication/communicator.h"
#include "helpers/logging.h"
#include "helpers/storageHelper.h"
#include "helpers/storageHelperCreator.h"
#include "readCache.h"
#include "scheduler.h"
#include "writeBuffer.h"

#include <chrono>
#include <memory>
#include <mutex>

namespace one {
namespace helpers {
namespace buffering {

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
    BufferAgentsMemoryLimitGuard(const BufferLimits &bufferLimits)
        : m_bufferLimits{bufferLimits}
        , m_readBuffersReservedSize{0}
        , m_writeBuffersReservedSize{0}
    {
    }

    /**
     * This function tries to mark readSize and writeSize bytes as reserved with
     * respect to total buffer limits specified in the application buffer
     * limits. No actual memory allocation is done here. If this function
     * returns false, it means that the global buffer memory limit has been
     * exhausted and buffered file handles cannot be created until some other
     * are closed.
     * @param readSize The size in bytes of maximum memory needed by the read
     * buffer
     * @param writeSize The size in bytes of maximum memory needed by the write
     * buffer
     */
    bool reserveBuffers(size_t readSize, size_t writeSize)
    {
        std::lock_guard<std::mutex> lock{m_mutex};

        if (((m_bufferLimits.readBuffersTotalSize == 0) ||
                (m_readBuffersReservedSize + readSize <=
                    m_bufferLimits.readBuffersTotalSize)) &&
            ((m_bufferLimits.writeBuffersTotalSize == 0) ||
                (m_writeBuffersReservedSize + writeSize <=
                    m_bufferLimits.writeBuffersTotalSize))) {
            m_readBuffersReservedSize += readSize;
            m_writeBuffersReservedSize += writeSize;
            return true;
        }

        return false;
    }

    /**
     * This function tries to release the readSize and writeSize bytes from the
     * memory limit guard.
     * @param readSize The size in bytes of maximum memory used by the read
     * buffer
     * @param writeSize The size in bytes of maximum memory used by the write
     * buffer
     */
    bool releaseBuffers(size_t readSize, size_t writeSize)
    {
        std::lock_guard<std::mutex> lock{m_mutex};

        if ((m_readBuffersReservedSize - readSize >= 0) &&
            (m_writeBuffersReservedSize - writeSize >= 0)) {
            m_readBuffersReservedSize -= readSize;
            m_writeBuffersReservedSize -= writeSize;
            return true;
        }

        return false;
    }

private:
    const BufferLimits m_bufferLimits;

    std::mutex m_mutex;
    size_t m_readBuffersReservedSize;
    size_t m_writeBuffersReservedSize;
};

class BufferAgent;

class BufferedFileHandle : public FileHandle {
public:
    BufferedFileHandle(folly::fbstring fileId, FileHandlePtr wrappedHandle,
        const BufferLimits &bl, Scheduler &scheduler,
        std::shared_ptr<BufferAgent> bufferAgent,
        std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard);

    ~BufferedFileHandle()
    {
        if (m_bufferMemoryLimitGuard) {
            m_bufferMemoryLimitGuard->releaseBuffers(
                m_bufferLimits.readBufferMaxSize,
                m_bufferLimits.writeBufferMaxSize);
        }
    }

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override
    {
        return read(offset, size, std::numeric_limits<off_t>::max() - offset);
    }

    folly::Future<folly::IOBufQueue> read(const off_t offset,
        const std::size_t size, const std::size_t continuousSize) override
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size)
                    << LOG_FARG(continuousSize);

        DCHECK(continuousSize >= size);

        // Push all changes so we'll always read data that we just wrote. A
        // mechanism in `WriteBuffer` will trigger a clear of the readCache if
        // needed. This might be optimized in the future by modifying readcache
        // on write.
        return m_writeBuffer->fsync().then(
            [=] { return m_readCache->read(offset, size, continuousSize); });
    }

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

        return m_writeBuffer->write(offset, std::move(buf), std::move(writeCb));
    }

    folly::Future<folly::Unit> fsync(bool isDataSync) override
    {
        LOG_FCALL() << LOG_FARG(isDataSync);

        return m_writeBuffer->fsync().then(
            [readCache = m_readCache, wrappedHandle = m_wrappedHandle,
                isDataSync] {
                readCache->clear();
                return wrappedHandle->fsync(isDataSync);
            });
    }

    folly::Future<folly::Unit> flush() override
    {
        return m_writeBuffer->fsync().then(
            [readCache = m_readCache, wrappedHandle = m_wrappedHandle] {
                readCache->clear();
                return wrappedHandle->flush();
            });
    }

    folly::Future<folly::Unit> release() override
    {
        LOG_FCALL();

        return m_writeBuffer->fsync().then(
            [wrappedHandle = m_wrappedHandle] { wrappedHandle->release(); });
    }

    const Timeout &timeout() override { return m_wrappedHandle->timeout(); }

    bool needsDataConsistencyCheck() override
    {
        return m_wrappedHandle->needsDataConsistencyCheck();
    }

    std::size_t wouldPrefetch(
        const off_t offset, const std::size_t size) override
    {
        return m_readCache->wouldPrefetch(offset, size);
    }

    folly::Future<folly::Unit> flushUnderlying() override
    {
        return m_wrappedHandle->flush();
    }

    FileHandlePtr wrappedHandle() { return m_wrappedHandle; }

    virtual folly::Future<folly::Unit> refreshHelperParams(
        std::shared_ptr<StorageHelperParams> params) override
    {
        return m_wrappedHandle->refreshHelperParams(std::move(params));
    }

private:
    FileHandlePtr m_wrappedHandle;
    BufferLimits m_bufferLimits;
    Scheduler &m_scheduler;
    std::shared_ptr<ReadCache> m_readCache;
    std::shared_ptr<WriteBuffer> m_writeBuffer;
    std::shared_ptr<BufferAgentsMemoryLimitGuard> m_bufferMemoryLimitGuard;
};

class BufferAgent : public StorageHelper,
                    public std::enable_shared_from_this<BufferAgent> {
public:
    BufferAgent(BufferLimits bufferLimits, StorageHelperPtr helper,
        Scheduler &scheduler,
        std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard)
        : m_bufferLimits{std::move(bufferLimits)}
        , m_helper{std::move(helper)}
        , m_scheduler{scheduler}
        , m_bufferMemoryLimitGuard{std::move(bufferMemoryLimitGuard)}
    {
        LOG_FCALL() << LOG_FARG(bufferLimits.readBufferMinSize)
                    << LOG_FARG(bufferLimits.readBufferMaxSize)
                    << LOG_FARG(bufferLimits.readBufferPrefetchDuration.count())
                    << LOG_FARG(bufferLimits.writeBufferMinSize)
                    << LOG_FARG(bufferLimits.writeBufferMaxSize)
                    << LOG_FARG(bufferLimits.writeBufferFlushDelay.count());
    }

    folly::fbstring name() const override { return m_helper->name(); }

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &params) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(flags)
                    << LOG_FARGM(params);

        return m_helper->open(fileId, flags, params)
            .then(
                [fileId, agent = shared_from_this(), bl = m_bufferLimits,
                    memoryLimitGuard = m_bufferMemoryLimitGuard,
                    &scheduler = m_scheduler](FileHandlePtr handle) {
                    if (memoryLimitGuard->reserveBuffers(
                            bl.readBufferMaxSize, bl.writeBufferMaxSize)) {
                        return static_cast<FileHandlePtr>(
                            std::make_shared<BufferedFileHandle>(
                                std::move(fileId), std::move(handle), bl,
                                scheduler, std::move(agent), memoryLimitGuard));
                    }

                    LOG_DBG(1)
                        << "Couldn't create buffered file handle for file "
                        << fileId
                        << " due to exhausted overall buffer limit by already "
                           "opened files.";

                    return handle;
                });
    }

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override
    {
        LOG_FCALL() << LOG_FARG(fileId);

        return m_helper->getattr(fileId);
    }

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mask);

        return m_helper->access(fileId, mask);
    }

    folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override
    {
        LOG_FCALL() << LOG_FARG(fileId);

        return m_helper->readlink(fileId);
    }

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

        return m_helper->readdir(fileId, offset, count);
    }

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mode);

        return m_helper->mknod(fileId, mode, flags, rdev);
    }

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mode);

        return m_helper->mkdir(fileId, mode);
    }

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(currentSize);

        return m_helper->unlink(fileId, currentSize);
    }

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override
    {
        LOG_FCALL() << LOG_FARG(fileId);

        return m_helper->rmdir(fileId);
    }

    folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to) override
    {
        LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

        return m_helper->symlink(from, to);
    }

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override
    {
        LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

        return m_helper->rename(from, to);
    }

    folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to) override
    {
        LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

        return m_helper->link(from, to);
    }

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mode);

        return m_helper->chmod(fileId, mode);
    }

    folly::Future<folly::Unit> chown(const folly::fbstring &fileId,
        const uid_t uid, const gid_t gid) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

        return m_helper->chown(fileId, uid, gid);
    }

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size)
                    << LOG_FARG(currentSize);

        return m_helper->truncate(fileId, size, currentSize);
    }

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override
    {
        LOG_FCALL() << LOG_FARG(uuid) << LOG_FARG(name);

        return m_helper->getxattr(uuid, name);
    }

    folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override
    {
        LOG_FCALL() << LOG_FARG(uuid) << LOG_FARG(name) << LOG_FARG(value)
                    << LOG_FARG(create) << LOG_FARG(replace);

        return m_helper->setxattr(uuid, name, value, create, replace);
    }

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override
    {
        LOG_FCALL() << LOG_FARG(uuid) << LOG_FARG(name);

        return m_helper->removexattr(uuid, name);
    }

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid) override
    {
        LOG_FCALL() << LOG_FARG(uuid);

        return m_helper->listxattr(uuid);
    }

    virtual folly::Future<std::shared_ptr<StorageHelperParams>>
    params() const override
    {
        return m_helper->params();
    }

    virtual folly::Future<folly::Unit> refreshParams(
        std::shared_ptr<StorageHelperParams> params) override
    {
        LOG_FCALL();
        return m_helper->refreshParams(std::move(params));
    }

    StorageHelperPtr helper() { return m_helper; }

    const Timeout &timeout() override { return m_helper->timeout(); }

private:
    BufferLimits m_bufferLimits;
    StorageHelperPtr m_helper;
    Scheduler &m_scheduler;
    std::shared_ptr<BufferAgentsMemoryLimitGuard> m_bufferMemoryLimitGuard;
};

} // namespace proxyio
} // namespace helpers
} // namespace one

#endif // HELPERS_BUFFERING_BUFFER_AGENT_H
