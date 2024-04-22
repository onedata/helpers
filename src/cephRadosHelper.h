/**
 * @file cephRadosHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_CEPHRADOS_HELPER_H
#define HELPERS_CEPHRADOS_HELPER_H

#include "cephRadosHelperParams.h"
#include "keyValueAdapter.h"
#include "keyValueHelper.h"

#include <folly/ThreadLocal.h>
#include <folly/executors/IOExecutor.h>
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
    explicit CephRadosHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
    }

    folly::fbstring name() const override { return CEPHRADOS_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"monitorHostname", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        auto params = CephRadosHelperParams::create(parameters);

        return std::make_shared<KeyValueAdapter>(
            std::make_shared<CephRadosHelper>(params), params, m_executor,
            executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
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
    using params_type = CephRadosHelperParams;

    /**
     * Constructor.
     * @param clusterName Name of the Ceph cluster to connect to.
     * @param monHost Name of the Ceph monitor host.
     * @param poolName Name of the Ceph pool to use.
     * @param userName Name of the Ceph user.
     * @param key Secret key of the Ceph user.
     * @param timeout Asynchronous operations timeout.
     */
    explicit CephRadosHelper(std::shared_ptr<CephRadosHelperParams> params);

    CephRadosHelper(const CephRadosHelper &) = delete;
    CephRadosHelper &operator=(const CephRadosHelper &) = delete;
    CephRadosHelper(CephRadosHelper &&) = delete;
    CephRadosHelper &operator=(CephRadosHelper &&) = delete;

    ~CephRadosHelper() = default;

    folly::fbstring name() const override { return CEPHRADOS_HELPER_NAME; };

    bool supportsBatchDelete() const override { return false; }

    folly::IOBufQueue getObject(const folly::fbstring &key, const off_t offset,
        const std::size_t size) override;

    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    void deleteObject(const folly::fbstring &key) override;

    void deleteObjects(const folly::fbvector<folly::fbstring> &keys) override;

    HELPER_PARAM_GETTER(clusterName);
    HELPER_PARAM_GETTER(monitorHostname);
    HELPER_PARAM_GETTER(poolName);
    HELPER_PARAM_GETTER(username);
    HELPER_PARAM_GETTER(key);

private:
    std::shared_ptr<params_type> P() const
    {
        return std::dynamic_pointer_cast<params_type>(params().get());
    }

    void connect();

    std::mutex m_connectionMutex;
    folly::ThreadLocal<CephRadosCtx> m_ctx;
};
} // namespace helpers
} // namespace one

#endif // HELPERS_CEPHRADOS_HELPER_H
