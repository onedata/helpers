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

#include <folly/executors/IOExecutor.h>

#include <cstdint>

namespace one {
namespace helpers {

class ProxyHelper;

/**
 * The @c FileHandle implementation for Proxy storage helper.
 */
class ProxyFileHandle : public FileHandle {
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
    ProxyFileHandle(folly::fbstring fileId, folly::fbstring storageId,
        Params openParams, communication::Communicator &communicator,
        std::shared_ptr<ProxyHelper> helper, Timeout timeout);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    folly::Future<std::size_t> multiwrite(
        folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>>
            buffs) override;

    const Timeout &timeout() override { return m_timeout; }

    bool isConcurrencyEnabled() const override { return true; }

private:
    folly::fbstring m_storageId;
    communication::Communicator &m_communicator;
    Timeout m_timeout;
};

/**
 * @c ProxyHelper is responsible for providing a POSIX-like API for operations
 * on files proxied through a onedata provider.
 */
class ProxyHelper : public StorageHelper,
                    public std::enable_shared_from_this<ProxyHelper> {
public:
    /**
     * Constructor.
     * @param storageId Id of the storage the file is stored on.
     * @param communicator Communicator that will be used for communication
     * with a provider.
     */
    ProxyHelper(folly::fbstring storageId,
        communication::Communicator &communicator,
        Timeout timeout = ASYNC_OPS_TIMEOUT,
        ExecutionContext executionContext = ExecutionContext::ONECLIENT);

    folly::fbstring name() const override { return PROXY_HELPER_NAME; };

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    const Timeout &timeout() override { return m_timeout; }

    std::shared_ptr<folly::Executor> executor() override
    {
        return m_communicator.executor();
    };

private:
    folly::fbstring m_storageId;
    communication::Communicator &m_communicator;
    Timeout m_timeout;
};

/**
 * An implementation of @c StorageHelperFactory for Proxy storage helper.
 */
class ProxyHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param communicator Communicator that will be used for communication
     * with a provider.
     */
    ProxyHelperFactory(communication::Communicator &communicator)
        : m_communicator{communicator}
    {
    }

    virtual folly::fbstring name() const override { return PROXY_HELPER_NAME; }

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters,
        ExecutionContext executionContext =
            ExecutionContext::ONEPROVIDER) override
    {
        auto storageId = getParam(parameters, "storageId");
        Timeout timeout{getParam<std::size_t>(
            parameters, "timeout", ASYNC_OPS_TIMEOUT.count())};

        return std::make_shared<ProxyHelper>(std::move(storageId),
            m_communicator, std::move(timeout), executionContext);
    }

private:
    communication::Communicator &m_communicator;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_PROXY_IO_HELPER_H
