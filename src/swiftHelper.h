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
#include "swiftHelperParams.h"

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
    explicit SwiftHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
    }

    folly::fbstring name() const override { return SWIFT_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"authUrl", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        auto params = SwiftHelperParams::create(parameters);

        return std::make_shared<KeyValueAdapter>(
            std::make_shared<SwiftHelper>(params), params, m_executor,
            executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

/**
 * The SwiftHelper class provides access to Swift storage system.
 */
class SwiftHelper : public KeyValueHelper {
public:
    using params_type = SwiftHelperParams;

    /**
     * Constructor.
     * @param authUrl The URL for authorization with Swift.
     * @param containerName Name of the used container.
     * @param tenantName Name of the tenant.
     * @param userName Name of the Swift user.
     * @param password Password of the Swift user.
     */
    explicit SwiftHelper(std::shared_ptr<SwiftHelperParams> params);

    SwiftHelper(const SwiftHelper &) = delete;
    SwiftHelper &operator=(const SwiftHelper &) = delete;
    SwiftHelper(SwiftHelper &&) = delete;
    SwiftHelper &operator=(SwiftHelper &&) = delete;

    virtual ~SwiftHelper() = default;

    folly::fbstring name() const override { return SWIFT_HELPER_NAME; };

    HELPER_PARAM_GETTER(authUrl)
    HELPER_PARAM_GETTER(containerName)
    HELPER_PARAM_GETTER(tenantName)
    HELPER_PARAM_GETTER(username)
    HELPER_PARAM_GETTER(password)

    bool supportsBatchDelete() const override { return true; }

    folly::IOBufQueue getObject(const folly::fbstring &key, const off_t offset,
        const std::size_t size) override;

    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    void deleteObject(const folly::fbstring &key) override;

    void deleteObjects(const folly::fbvector<folly::fbstring> &keys) override;

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
    };

    std::unique_ptr<Authentication> m_auth;

    folly::fbstring m_containerName;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_SWIFT_HELPER_H
