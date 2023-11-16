/**
 * @file bufferAgent.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "bufferAgent.h"

namespace one {
namespace helpers {
namespace buffering {

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
} // namespace buffering
} // namespace helpers
} // namespace one
