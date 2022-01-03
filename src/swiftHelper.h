/**
 * @file swiftHelper.h
 * @author Michal Wrona
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_SWIFT_HELPER_H
#define HELPERS_SWIFT_HELPER_H

#include "keyValueAdapter.h"
#include "keyValueHelper.h"

#include "Swift/Account.h"
#include "Swift/Container.h"
#include "Swift/HTTPIO.h"
#include "Swift/Object.h"

#include <folly/executors/IOExecutor.h>

#include <mutex>
#include <vector>

namespace one {
namespace helpers {

class SwiftHelper;

/**
 * An implementation of @c StorageHelperFactory for Swift storage helper.
 */
class SwiftHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param executor executor that will be used for some async operations.
     */
    SwiftHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
    }

    virtual folly::fbstring name() const override { return SWIFT_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"authUrl", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        const auto &authUrl = getParam(parameters, "authUrl");
        const auto &containerName = getParam(parameters, "containerName");
        const auto &tenantName = getParam(parameters, "tenantName");
        const auto &userName = getParam(parameters, "username");
        const auto &password = getParam(parameters, "password");
        const auto storagePathType =
            getParam<StoragePathType>(parameters, "storagePathType");
        Timeout timeout{getParam<std::size_t>(
            parameters, "timeout", constants::ASYNC_OPS_TIMEOUT.count())};
        const auto &blockSize =
            getParam<std::size_t>(parameters, "blockSize", DEFAULT_BLOCK_SIZE);

        return std::make_shared<KeyValueAdapter>(
            std::make_shared<SwiftHelper>(containerName, authUrl, tenantName,
                userName, password, std::move(timeout), storagePathType),
            m_executor, blockSize, executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

/**
 * The SwiftHelper class provides access to Swift storage system.
 */
class SwiftHelper : public KeyValueHelper {
public:
    /**
     * Constructor.
     * @param authUrl The URL for authorization with Swift.
     * @param containerName Name of the used container.
     * @param tenantName Name of the tenant.
     * @param userName Name of the Swift user.
     * @param password Password of the Swift user.
     */
    SwiftHelper(folly::fbstring containerName, const folly::fbstring &authUrl,
        const folly::fbstring &tenantName, const folly::fbstring &userName,
        const folly::fbstring &password,
        Timeout timeout = constants::ASYNC_OPS_TIMEOUT,
        StoragePathType storagePathType = StoragePathType::FLAT);

    folly::fbstring name() const override { return SWIFT_HELPER_NAME; };

    bool supportsBatchDelete() const override { return true; }

    folly::IOBufQueue getObject(const folly::fbstring &key, const off_t offset,
        const std::size_t size) override;

    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    void deleteObject(const folly::fbstring &key) override;

    void deleteObjects(const folly::fbvector<folly::fbstring> &keys) override;

    const Timeout &timeout() override { return m_timeout; }

private:
    class Authentication {
    public:
        Authentication(const folly::fbstring &authUrl,
            const folly::fbstring &tenantName, const folly::fbstring &userName,
            const folly::fbstring &password);

        Swift::Account &getAccount();

    private:
        std::mutex m_authMutex;
        Swift::AuthenticationInfo m_authInfo;
        std::shared_ptr<Swift::Account> m_account;
    } m_auth;

    folly::fbstring m_containerName;
    Timeout m_timeout;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_SWIFT_HELPER_H
