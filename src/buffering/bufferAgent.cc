/**
 * @file bufferAgent.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "bufferAgent.h"

#include "readCache.h"
#include "writeBuffer.h"

namespace one {
namespace helpers {
namespace buffering {

BufferAgentsMemoryLimitGuard::BufferAgentsMemoryLimitGuard(
    const BufferLimits &bufferLimits)
    : m_bufferLimits{bufferLimits}
    , m_readBuffersReservedSize{0}
    , m_writeBuffersReservedSize{0}
{
}

bool BufferAgentsMemoryLimitGuard::reserveBuffers(
    size_t readSize, size_t writeSize)
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

        LOG_DBG(3) << "Reserved helper buffers - read: " << readSize
                   << " write: " << writeSize;

        return true;
    }

    LOG_DBG(3)
        << "Couldn't reserve buffers for helper - memory limits exhausted";

    return false;
}

bool BufferAgentsMemoryLimitGuard::releaseBuffers(
    size_t readSize, size_t writeSize)
{
    std::lock_guard<std::mutex> lock{m_mutex};

    LOG_DBG(3) << "Releasing memory for helper buffer - read: " << readSize
               << " write: " << writeSize;

    if (m_readBuffersReservedSize - readSize >= 0)
        m_readBuffersReservedSize -= readSize;
    else
        m_readBuffersReservedSize = 0;

    if (m_writeBuffersReservedSize - writeSize >= 0)
        m_writeBuffersReservedSize -= writeSize;
    else
        m_writeBuffersReservedSize = 0;

    return true;
}

BufferedFileHandle::BufferedFileHandle(const folly::fbstring &fileId,
    FileHandlePtr wrappedHandle, const BufferLimits &bl,
    std::shared_ptr<Scheduler> scheduler,
    std::shared_ptr<BufferAgent> bufferAgent,
    std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard)
    : FileHandle{fileId, std::move(bufferAgent)}
    , m_wrappedHandle{std::move(wrappedHandle)}
    , m_bufferLimits{bl}
    , m_readCache{std::make_shared<ReadCache>(bl.readBufferMinSize,
          bl.readBufferMaxSize, bl.readBufferPrefetchDuration,
          bl.prefetchPowerBase, bl.targetLatency, *m_wrappedHandle)}
    , m_writeBuffer{std::make_shared<WriteBuffer>(bl.writeBufferMinSize,
          bl.writeBufferMaxSize, bl.writeBufferFlushDelay, *m_wrappedHandle,
          scheduler, m_readCache)}
    , m_bufferMemoryLimitGuard{std::move(bufferMemoryLimitGuard)}
    , m_scheduler{std::move(scheduler)}
{
    LOG_FCALL() << LOG_FARG(fileId);
}

BufferedFileHandle::~BufferedFileHandle()
{
    LOG_FCALL();

    if (m_bufferMemoryLimitGuard) {
        m_bufferMemoryLimitGuard->releaseBuffers(
            m_bufferLimits.readBufferMaxSize,
            m_bufferLimits.writeBufferMaxSize);
    }
}

folly::Future<folly::IOBufQueue> BufferedFileHandle::read(
    const off_t offset, const std::size_t size)
{
    return readContinuous(
        offset, size, std::numeric_limits<off_t>::max() - offset);
}

folly::Future<folly::IOBufQueue> BufferedFileHandle::readContinuous(
    const off_t offset, const std::size_t size,
    const std::size_t continuousSize)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size)
                << LOG_FARG(continuousSize);

    DCHECK(continuousSize >= size);

    // Push all changes so we'll always read data that we just wrote. A
    // mechanism in `WriteBuffer` will trigger a clear of the readCache if
    // needed. This might be optimized in the future by modifying readcache
    // on write.
    return m_writeBuffer->fsync().thenValue([=](auto && /*unit*/) {
        return m_readCache->read(offset, size, continuousSize);
    });
}

folly::Future<std::size_t> BufferedFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    return m_writeBuffer->write(offset, std::move(buf), std::move(writeCb));
}

folly::Future<folly::Unit> BufferedFileHandle::fsync(bool isDataSync)
{
    LOG_FCALL() << LOG_FARG(isDataSync);

    return m_writeBuffer->fsync().thenValue(
        [readCache = m_readCache, wrappedHandle = m_wrappedHandle, isDataSync](
            auto && /*unit*/) {
            readCache->clear();
            return wrappedHandle->fsync(isDataSync);
        });
}

folly::Future<folly::Unit> BufferedFileHandle::flush()
{
    LOG_FCALL();

    return m_writeBuffer->fsync().thenValue(
        [readCache = m_readCache, wrappedHandle = m_wrappedHandle](
            auto && /*unit*/) {
            readCache->clear();
            return wrappedHandle->flush();
        });
}

folly::Future<folly::Unit> BufferedFileHandle::release()
{
    LOG_FCALL();

    return m_writeBuffer->fsync().thenValue(
        [wrappedHandle = m_wrappedHandle](
            auto && /*unit*/) { wrappedHandle->release(); });
}

const Timeout &BufferedFileHandle::timeout()
{
    return m_wrappedHandle->timeout();
}

bool BufferedFileHandle::needsDataConsistencyCheck()
{
    return m_wrappedHandle->needsDataConsistencyCheck();
}

std::size_t BufferedFileHandle::wouldPrefetch(
    const off_t offset, const std::size_t size)
{
    return m_readCache->wouldPrefetch(offset, size);
}

folly::Future<folly::Unit> BufferedFileHandle::flushUnderlying()
{
    return m_wrappedHandle->flush();
}

FileHandlePtr BufferedFileHandle::wrappedHandle() { return m_wrappedHandle; }

folly::Future<folly::Unit> BufferedFileHandle::refreshHelperParams(
    std::shared_ptr<StorageHelperParams> params)
{
    return m_wrappedHandle->refreshHelperParams(std::move(params));
}

BufferAgent::BufferAgent(BufferLimits bufferLimits, StorageHelperPtr helper,
    std::shared_ptr<Scheduler> scheduler,
    std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard)
    : m_bufferLimits{bufferLimits}
    , m_helper{std::move(helper)}
    , m_scheduler{std::move(scheduler)}
    , m_bufferMemoryLimitGuard{std::move(bufferMemoryLimitGuard)}
{
    LOG_FCALL() << LOG_FARG(bufferLimits.readBufferMinSize)
                << LOG_FARG(bufferLimits.readBufferMaxSize)
                << LOG_FARG(bufferLimits.readBufferPrefetchDuration.count())
                << LOG_FARG(bufferLimits.writeBufferMinSize)
                << LOG_FARG(bufferLimits.writeBufferMaxSize)
                << LOG_FARG(bufferLimits.writeBufferFlushDelay.count());
}

folly::fbstring BufferAgent::name() const { return m_helper->name(); }

std::shared_ptr<folly::Executor> BufferAgent::executor()
{
    return m_helper->executor();
}

folly::Future<FileHandlePtr> BufferAgent::open(
    const folly::fbstring &fileId, const int flags, const Params &params)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(flags) << LOG_FARGM(params);

    return m_helper->open(fileId, flags, params)
        .thenValue([fileId, agent = shared_from_this(), bl = m_bufferLimits,
                       memoryLimitGuard = m_bufferMemoryLimitGuard,
                       scheduler = m_scheduler](
                       FileHandlePtr &&handle) mutable {
            if (memoryLimitGuard->reserveBuffers(
                    bl.readBufferMaxSize, bl.writeBufferMaxSize)) {
                return static_cast<FileHandlePtr>(
                    std::make_shared<BufferedFileHandle>(fileId,
                        std::move(handle), bl, scheduler, std::move(agent),
                        memoryLimitGuard));
            }

            LOG_DBG(1) << "Couldn't create buffered file handle for file "
                       << fileId
                       << " due to exhausted overall buffer limit by already "
                          "opened files.";

            return std::move(handle);
        });
}

folly::Future<struct stat> BufferAgent::getattr(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return m_helper->getattr(fileId);
}

folly::Future<folly::Unit> BufferAgent::access(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mask);

    return m_helper->access(fileId, mask);
}

folly::Future<folly::fbstring> BufferAgent::readlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return m_helper->readlink(fileId);
}

folly::Future<folly::fbvector<folly::fbstring>> BufferAgent::readdir(
    const folly::fbstring &fileId, const off_t offset, const std::size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return m_helper->readdir(fileId, offset, count);
}

folly::Future<folly::Unit> BufferAgent::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet &flags, const dev_t rdev)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mode);

    return m_helper->mknod(fileId, mode, flags, rdev);
}

folly::Future<folly::Unit> BufferAgent::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mode);

    return m_helper->mkdir(fileId, mode);
}

folly::Future<folly::Unit> BufferAgent::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(currentSize);

    return m_helper->unlink(fileId, currentSize);
}

folly::Future<folly::Unit> BufferAgent::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return m_helper->rmdir(fileId);
}

folly::Future<folly::Unit> BufferAgent::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return m_helper->symlink(from, to);
}

folly::Future<folly::Unit> BufferAgent::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return m_helper->rename(from, to);
}

folly::Future<folly::Unit> BufferAgent::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return m_helper->link(from, to);
}

folly::Future<folly::Unit> BufferAgent::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGO(mode);

    return m_helper->chmod(fileId, mode);
}

folly::Future<folly::Unit> BufferAgent::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    return m_helper->chown(fileId, uid, gid);
}

folly::Future<folly::Unit> BufferAgent::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size) << LOG_FARG(currentSize);

    return m_helper->truncate(fileId, size, currentSize);
}

folly::Future<folly::fbstring> BufferAgent::getxattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(uuid) << LOG_FARG(name);

    return m_helper->getxattr(uuid, name);
}

folly::Future<folly::Unit> BufferAgent::setxattr(const folly::fbstring &uuid,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace)
{
    LOG_FCALL() << LOG_FARG(uuid) << LOG_FARG(name) << LOG_FARG(value)
                << LOG_FARG(create) << LOG_FARG(replace);

    return m_helper->setxattr(uuid, name, value, create, replace);
}

folly::Future<folly::Unit> BufferAgent::removexattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(uuid) << LOG_FARG(name);

    return m_helper->removexattr(uuid, name);
}

folly::Future<folly::fbvector<folly::fbstring>> BufferAgent::listxattr(
    const folly::fbstring &uuid)
{
    LOG_FCALL() << LOG_FARG(uuid);

    return m_helper->listxattr(uuid);
}

folly::Future<std::shared_ptr<StorageHelperParams>> BufferAgent::params() const
{
    return m_helper->params();
}

folly::Future<folly::Unit> BufferAgent::refreshParams(
    std::shared_ptr<StorageHelperParams> params)
{
    LOG_FCALL();
    return m_helper->refreshParams(std::move(params));
}

StorageHelperPtr BufferAgent::helper() { return m_helper; }

const Timeout &BufferAgent::timeout() { return m_helper->timeout(); }

} // namespace buffering
} // namespace helpers
} // namespace one
