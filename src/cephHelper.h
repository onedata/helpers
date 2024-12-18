/**
 * @file cephHelper.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_CEPH_HELPER_H
#define HELPERS_CEPH_HELPER_H

#include "helpers/storageHelper.h"

#include "cephHelperParams.h"
#include "helpers/logging.h"

#include <folly/executors/IOExecutor.h>
#include <rados/librados.hpp>
#include <radosstriper/libradosstriper.hpp>

namespace one {
namespace helpers {

class CephHelper;

constexpr auto CEPH_STRIPER_FIRST_OBJECT_SUFFIX = ".0000000000000000";
constexpr auto CEPH_STRIPER_LOCK_NAME = "striper.lock";

/**
 * The @c FileHandle implementation for Ceph storage helper.
 */
class CephFileHandle : public FileHandle,
                       public std::enable_shared_from_this<CephFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId Ceph-specific ID associated with the file.
     * @param helper A pointer to the helper that created the handle.
     * @param ioCTX A reference to @c librados::IoCtx for async operations.
     */
    CephFileHandle(folly::fbstring fileId, std::shared_ptr<CephHelper> helper);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    const Timeout &timeout() override;
};

/**
 * The CephHelper class provides access to Ceph storage via librados library.
 */
class CephHelper : public StorageHelper,
                   public std::enable_shared_from_this<CephHelper> {
public:
    using params_type = CephHelperParams;

    /**
     * Constructor.
     * @param params
     * @param executor Executor that will drive the helper's async operations.
     */
    CephHelper(std::shared_ptr<CephHelperParams> params,
        std::shared_ptr<folly::Executor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    CephHelper(const CephHelper &) = delete;
    CephHelper &operator=(const CephHelper &) = delete;
    CephHelper(CephHelper &&) = delete;
    CephHelper &operator=(CephHelper &&) = delete;

    /**
     * Destructor.
     * Closes connection to Ceph storage cluster and destroys internal context
     * object.
     */
    ~CephHelper();

    folly::fbstring name() const override { return CEPH_HELPER_NAME; };

    folly::Future<folly::Unit> checkStorageAvailability() override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int /*flags*/, const Params & /*openParams*/) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId) override;

    HELPER_PARAM_GETTER(clusterName);
    HELPER_PARAM_GETTER(monitorHostname);
    HELPER_PARAM_GETTER(poolName);
    HELPER_PARAM_GETTER(username);
    HELPER_PARAM_GETTER(key);

    std::shared_ptr<folly::Executor> executor() override { return m_executor; }

    libradosstriper::RadosStriper &getRadosStriper() { return m_radosStriper; }

    librados::IoCtx &getIoCTX() { return m_ioCTX; }

    /**
     * Establishes connection to the Ceph storage cluster.
     */
    folly::Future<folly::Unit> connect();

private:
    /**
     * Forcibly removes any locks on the first object of a larger object managed
     * by Rados striper.
     * @param fileId The name of the object recognizable by Rados striper
     * @returns 0 on success, otherwise an error code
     */
    int removeStriperLocks(const folly::fbstring &fileId);

    const size_t m_stripeUnit{4 * 1024 * 1024};
    const size_t m_stripeCount{8};
    const size_t m_objectSize{16 * 1024 * 1024};

    std::shared_ptr<folly::Executor> m_executor;

    librados::Rados m_cluster;
    librados::IoCtx m_ioCTX;
    libradosstriper::RadosStriper m_radosStriper;
    std::mutex m_connectionMutex;
    bool m_connected = false;
};

/**
 * An implementation of @c StorageHelperFactory for Ceph storage helper.
 */
class CephHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async operations.
     */
    explicit CephHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
        LOG_FCALL();
    }

    folly::fbstring name() const override { return CEPH_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"monitorHostname", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        auto params = CephHelperParams::create(parameters);
        return std::make_shared<CephHelper>(
            std::move(params), m_executor, executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_CEPH_HELPER_H
