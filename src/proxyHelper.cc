/**
 * @file proxyHelper.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "proxyHelper.h"
#include "monitoring/monitoring.h"

#include "messages/proxyio/remoteData.h"
#include "messages/proxyio/remoteRead.h"
#include "messages/proxyio/remoteWrite.h"
#include "messages/proxyio/remoteWriteResult.h"
#include "messages/status.h"

#include <asio/buffer.hpp>
#include <folly/futures/Future.h>

namespace one {
namespace helpers {

ProxyFileHandle::ProxyFileHandle(folly::fbstring fileId,
    folly::fbstring storageId, Params openParams,
    communication::Communicator &communicator,
    std::shared_ptr<ProxyHelper> helper, Timeout timeout)
    : FileHandle{fileId, openParams, std::move(helper)}
    , m_storageId{std::move(storageId)}
    , m_communicator{communicator}
    , m_timeout{timeout}
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(m_storageId)
                << LOG_FARGM(openParams);
}

folly::Future<folly::IOBufQueue> ProxyFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    messages::proxyio::RemoteRead msg{
        m_openParams, m_storageId, m_fileId, offset, size};

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.proxy.read");

    LOG_DBG(2) << "Attempting to read " << size << " bytes from file "
               << m_fileId;

    return m_communicator
        .communicate<messages::proxyio::RemoteData>(std::move(msg))
        .then([timer = std::move(timer)](
                  const messages::proxyio::RemoteData &rd) mutable {
            folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
            buf.append(rd.data());
            LOG_DBG(2) << "Received " << buf.chainLength()
                       << " bytes from provider";
            ONE_METRIC_TIMERCTX_STOP(timer, rd.data().size());
            return buf;
        });
}

folly::Future<std::size_t> ProxyFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    LOG_DBG(2) << "Attempting to write " << buf.chainLength()
               << " bytes to file " << m_fileId;

    folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>> buffs;
    buffs.emplace_back(
        std::make_tuple(offset, std::move(buf), std::move(writeCb)));
    return multiwrite(std::move(buffs));
}

folly::Future<std::size_t> ProxyFileHandle::multiwrite(
    folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>> buffs)
{
    LOG_FCALL() << LOG_FARG(buffs.size());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.proxy.write");

    LOG_DBG(2) << "Attempting multiwrite to file " << m_fileId << " from "
               << buffs.size() << " buffers";

    folly::fbvector<std::pair<WriteCallback, std::size_t>> postCallbacks;
    folly::fbvector<std::pair<off_t, folly::fbstring>> stringBuffs;
    stringBuffs.reserve(buffs.size());
    for (auto &buf : buffs) {
        if (!std::get<1>(buf).empty()) {
            postCallbacks.emplace_back(
                std::move(std::get<2>(buf)), std::get<1>(buf).chainLength());
            stringBuffs.emplace_back(
                std::get<0>(buf), std::get<1>(buf).move()->moveToFbString());
        }
    }

    messages::proxyio::RemoteWrite msg{
        m_openParams, m_storageId, m_fileId, std::move(stringBuffs)};

    return m_communicator
        .communicate<messages::proxyio::RemoteWriteResult>(std::move(msg))
        .within(m_timeout,
            std::system_error{std::make_error_code(std::errc::timed_out)})
        .then([timer = std::move(timer),
                  postCallbacks = std::move(postCallbacks)](
                  const messages::proxyio::RemoteWriteResult &result) mutable {
            ONE_METRIC_TIMERCTX_STOP(timer, result.wrote());

            LOG_DBG(2) << "Written " << result.wrote() << " bytes";

            for (auto &cb : postCallbacks)
                if (cb.first)
                    cb.first(cb.second);

            return result.wrote();
        });
}

ProxyHelper::ProxyHelper(folly::fbstring storageId,
    communication::Communicator &communicator, Timeout timeout,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_storageId{std::move(storageId)}
    , m_communicator{communicator}
    , m_timeout{timeout}
{
    LOG_FCALL() << LOG_FARG(m_storageId)
                << LOG_FARG(
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           timeout)
                           .count());
}

folly::Future<FileHandlePtr> ProxyHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGB(flags);

    LOG_DBG(2) << "Attempting to open file " << fileId << " with flags "
               << LOG_OCT(flags);

    return folly::makeFuture(static_cast<FileHandlePtr>(
        std::make_shared<ProxyFileHandle>(fileId, m_storageId, openParams,
            m_communicator, shared_from_this(), m_timeout)));
}

} // namespace helpers
} // namespace one
