/**
 * @file proxyHelper.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_PROXY_HELPER_H
#define HELPERS_PROXY_HELPER_H

#include "helpers/storageHelper.h"

#include "communication/communicator.h"
#include "monitoring/monitoring.h"

#include "messages/proxyio/remoteData.h"
#include "messages/proxyio/remoteRead.h"
#include "messages/proxyio/remoteWrite.h"
#include "messages/proxyio/remoteWriteResult.h"
#include "messages/status.h"

#include <folly/executors/IOExecutor.h>

#include <cstdint>

namespace one {
namespace helpers {

template <typename CommunicatorT> class ProxyHelper;

/**
 * The @c FileHandle implementation for Proxy storage helper.
 */
template <typename CommunicatorT> class ProxyFileHandle : public FileHandle {
public:
    /**
     * Constructor.
     * @param fileId Helper-specific ID of the open file.
     * @param storageId Id of the storage the file is stored on.
     * @param openParams Parameters associated with the handle.
     * @param communicator Communicator that will be used for communication
     * with a provider.
     * @param helper Shared ptr to underlying helper.
     */
    ProxyFileHandle(const folly::fbstring &fileId, folly::fbstring storageId,
        const Params &openParams, CommunicatorT &communicator,
        std::shared_ptr<ProxyHelper<CommunicatorT>> helper, Timeout timeout)
        : FileHandle{fileId, openParams, std::move(helper)}
        , m_storageId{std::move(storageId)}
        , m_communicator{communicator}
        , m_timeout{timeout}
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(m_storageId)
                    << LOG_FARGM(openParams);
    }

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

        messages::proxyio::RemoteRead msg{
            openParams(), m_storageId, fileId(), offset, size};

        auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.proxy.read");

        LOG_DBG(2) << "Attempting to read " << size << " bytes from file "
                   << fileId();

        return m_communicator
            .template communicate<messages::proxyio::RemoteData>(std::move(msg))
            .via(m_communicator.executor().get())
            .thenValue([timer = std::move(timer)](
                           messages::proxyio::RemoteData &&rd) mutable {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(rd.data());
                LOG_DBG(2) << "Received " << buf.chainLength()
                           << " bytes from provider";
                ONE_METRIC_TIMERCTX_STOP(timer, rd.data().size());
                return buf;
            });
    }

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override
    {
        LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

        LOG_DBG(2) << "Attempting to write " << buf.chainLength()
                   << " bytes to file " << fileId();

        folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>>
            buffs;
        buffs.emplace_back(
            std::make_tuple(offset, std::move(buf), std::move(writeCb)));
        return multiwrite(std::move(buffs));
    }

    folly::Future<std::size_t> multiwrite(
        folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>>
            buffs) override
    {
        LOG_FCALL() << LOG_FARG(buffs.size());

        auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.proxy.write");

        LOG_DBG(2) << "Attempting multiwrite to file " << fileId() << " from "
                   << buffs.size() << " buffers";

        folly::fbvector<std::pair<WriteCallback, std::size_t>> postCallbacks;
        folly::fbvector<std::pair<off_t, folly::fbstring>> stringBuffs;
        stringBuffs.reserve(buffs.size());
        for (auto &buf : buffs) {
            if (!std::get<1>(buf).empty()) {
                postCallbacks.emplace_back(std::move(std::get<2>(buf)),
                    std::get<1>(buf).chainLength());
                stringBuffs.emplace_back(std::get<0>(buf),
                    std::get<1>(buf).move()->moveToFbString());
            }
        }

        messages::proxyio::RemoteWrite msg{
            openParams(), m_storageId, fileId(), std::move(stringBuffs)};

        return m_communicator
            .template communicate<messages::proxyio::RemoteWriteResult>(
                std::move(msg))
            .via(m_communicator.executor().get())
            .within(m_timeout,
                std::system_error{std::make_error_code(std::errc::timed_out)})
            .thenValue(
                [timer = std::move(timer),
                    postCallbacks = std::move(postCallbacks)](
                    messages::proxyio::RemoteWriteResult &&result) mutable {
                    ONE_METRIC_TIMERCTX_STOP(timer, result.wrote());

                    LOG_DBG(2) << "Written " << result.wrote() << " bytes";

                    for (auto &cb : postCallbacks)
                        if (cb.first)
                            cb.first(cb.second);

                    return result.wrote();
                });
    }

    const Timeout &timeout() override { return m_timeout; }

    bool isConcurrencyEnabled() const override { return true; }

private:
    folly::fbstring m_storageId;
    CommunicatorT &m_communicator;
    Timeout m_timeout;
};

/**
 * @c ProxyHelper is responsible for providing a POSIX-like API for operations
 * on files proxied through a onedata provider.
 */
template <typename CommunicatorT>
class ProxyHelper
    : public StorageHelper,
      public std::enable_shared_from_this<ProxyHelper<CommunicatorT>> {
public:
    /**
     * Constructor.
     * @param storageId Id of the storage the file is stored on.
     * @param communicator Communicator that will be used for communication
     * with a provider.
     */
    ProxyHelper(folly::fbstring storageId, CommunicatorT &communicator,
        Timeout timeout = constants::ASYNC_OPS_TIMEOUT,
        ExecutionContext executionContext = ExecutionContext::ONECLIENT)
        : StorageHelper{executionContext}
        , m_storageId{std::move(storageId)}
        , m_communicator{communicator}
        , m_timeout{timeout}
    {
        LOG_FCALL()
            << LOG_FARG(m_storageId)
            << LOG_FARG(std::chrono::duration_cast<std::chrono::milliseconds>(
                   timeout)
                            .count());
    }

    folly::fbstring name() const override { return PROXY_HELPER_NAME; };

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override
    {
        LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGB(flags);

        LOG_DBG(2) << "Attempting to open file " << fileId << " with flags "
                   << LOG_OCT(flags);

        return folly::makeSemiFuture()
            .via(m_communicator.executor().get())
            .thenValue([fileId, openParams, this](auto && /*unit*/) {
                return std::dynamic_pointer_cast<FileHandle>(
                    std::make_shared<ProxyFileHandle<CommunicatorT>>(fileId,
                        m_storageId, openParams, m_communicator,
                        this->shared_from_this(), m_timeout));
            });
    }

    const Timeout &timeout() override { return m_timeout; }

    std::shared_ptr<folly::Executor> executor() override
    {
        return m_communicator.executor();
    };

private:
    folly::fbstring m_storageId;
    CommunicatorT &m_communicator;
    Timeout m_timeout;
};

/**
 * An implementation of @c StorageHelperFactory for Proxy storage helper.
 */
template <typename CommunicatorT>
class ProxyHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param communicator Communicator that will be used for communication
     * with a provider.
     */
    ProxyHelperFactory(CommunicatorT &communicator)
        : m_communicator{communicator}
    {
    }

    virtual folly::fbstring name() const override { return PROXY_HELPER_NAME; }

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        auto storageId = getParam(parameters, "storageId");
        Timeout timeout{getParam<std::size_t>(
            parameters, "timeout", constants::ASYNC_OPS_TIMEOUT.count())};

        return std::make_shared<ProxyHelper<CommunicatorT>>(
            std::move(storageId), m_communicator, std::move(timeout),
            executionContext);
    }

private:
    CommunicatorT &m_communicator;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_PROXY_IO_HELPER_H
