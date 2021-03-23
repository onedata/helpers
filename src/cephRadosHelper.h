/**
 * @file cephRadosHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_CEPHRADOS_HELPER_H
#define HELPERS_CEPHRADOS_HELPER_H

#include "asioExecutor.h"
#include "keyValueAdapter.h"
#include "keyValueHelper.h"

#include <folly/ThreadLocal.h>
#include <rados/librados.hpp>

#include <map>
#include <sstream>

namespace one {
namespace helpers {

class CephRadosHelper;

/**
 * An implementation of @c StorageHelperFactory for CephRados storage helper.
 */
class CephRadosHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async operations.
     */
    CephRadosHelperFactory(asio::io_service &service)
        : m_service{service}
    {
    }

    virtual folly::fbstring name() const override
    {
        return CEPHRADOS_HELPER_NAME;
    }

    const std::vector<folly::fbstring> overridableParams() const override
    {
        return {"monitorHostname", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters,
        ExecutionContext executionContext =
            ExecutionContext::ONEPROVIDER) override
    {
        const auto &clusterName = getParam(parameters, "clusterName");
        const auto &monHost = getParam(parameters, "monitorHostname");
        const auto &poolName = getParam(parameters, "poolName");
        const auto &userName = getParam(parameters, "username");
        const auto &key = getParam(parameters, "key");
        const auto storagePathType =
            getParam<StoragePathType>(parameters, "storagePathType");
        Timeout timeout{getParam<std::size_t>(
            parameters, "timeout", ASYNC_OPS_TIMEOUT.count())};
        const auto &blockSize =
            getParam<std::size_t>(parameters, "blockSize", DEFAULT_BLOCK_SIZE);

        return std::make_shared<KeyValueAdapter>(
            std::make_shared<CephRadosHelper>(clusterName, monHost, poolName,
                userName, key, std::move(timeout), storagePathType),
            std::make_shared<AsioExecutor>(m_service), blockSize,
            executionContext);
    }

private:
    asio::io_service &m_service;
};

struct CephRadosCtx {
    librados::IoCtx ioCTX;
    bool connected = false;
    librados::Rados cluster;
};

/**
 * The CephRadosHelper class provides access to Ceph object storage directly
 * using the RADOS API.
 */
class CephRadosHelper : public KeyValueHelper {
public:
    /**
     * Constructor.
     * @param clusterName Name of the Ceph cluster to connect to.
     * @param monHost Name of the Ceph monitor host.
     * @param poolName Name of the Ceph pool to use.
     * @param userName Name of the Ceph user.
     * @param key Secret key of the Ceph user.
     * @param timeout Asynchronous operations timeout.
     */
    CephRadosHelper(folly::fbstring clusterName, folly::fbstring monHost,
        folly::fbstring poolName, folly::fbstring userName, folly::fbstring key,
        Timeout timeout = ASYNC_OPS_TIMEOUT,
        StoragePathType storagePathType = StoragePathType::FLAT);

    folly::fbstring name() const override { return CEPHRADOS_HELPER_NAME; };

    bool supportsBatchDelete() const override { return false; }

    folly::IOBufQueue getObject(const folly::fbstring &key, const off_t offset,
        const std::size_t size) override;

    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    void deleteObject(const folly::fbstring &key) override;

    void deleteObjects(const folly::fbvector<folly::fbstring> &keys) override;

    const Timeout &timeout() override { return m_timeout; }

private:
    void connect();

    folly::fbstring m_clusterName;
    folly::fbstring m_monHost;
    folly::fbstring m_poolName;
    folly::fbstring m_userName;
    folly::fbstring m_key;

    Timeout m_timeout;

    std::mutex m_connectionMutex;
    folly::ThreadLocal<CephRadosCtx> m_ctx;
};
} // namespace helpers
} // namespace one

#endif // HELPERS_CEPHRADOS_HELPER_H
