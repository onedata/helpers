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

BufferedFileHandle::BufferedFileHandle(folly::fbstring fileId,
    FileHandlePtr wrappedHandle, const BufferLimits &bl, Scheduler &scheduler,
    std::shared_ptr<BufferAgent> bufferAgent,
    std::shared_ptr<BufferAgentsMemoryLimitGuard> bufferMemoryLimitGuard)
    : FileHandle{std::move(fileId), std::move(bufferAgent)}
    , m_wrappedHandle{std::move(wrappedHandle)}
    , m_bufferLimits{bl}
    , m_scheduler{scheduler}
    , m_readCache{std::make_shared<ReadCache>(bl.readBufferMinSize,
          bl.readBufferMaxSize, bl.readBufferPrefetchDuration,
          bl.prefetchPowerBase, bl.targetLatency, *m_wrappedHandle)}
    , m_writeBuffer{std::make_shared<WriteBuffer>(bl.writeBufferMinSize,
          bl.writeBufferMaxSize, bl.writeBufferFlushDelay, *m_wrappedHandle,
          m_scheduler, m_readCache)}
    , m_bufferMemoryLimitGuard{std::move(bufferMemoryLimitGuard)}
{
    LOG_FCALL() << LOG_FARG(fileId);
    m_writeBuffer->scheduleFlush();
}
} // namespace buffering
} // namespace helpers
} // namespace one
