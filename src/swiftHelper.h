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

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPResponse.h>
#include <folly/executors/IOExecutor.h>

#include <mutex>
#include <vector>

namespace one {
namespace helpers {
template <class T> struct SwiftResult {
    SwiftResult(T v,
        Poco::Net::HTTPResponse::HTTPStatus s =
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
    {
        value = std::move(v);
        httpStatus = s;
    }

    SwiftResult(Poco::Net::HTTPResponse::HTTPStatus s, std::string m = {})
    {
        httpStatus = s;
        msg = std::move(m);
    }

    Poco::Net::HTTPResponse::HTTPStatus httpStatus;
    folly::fbstring msg;
    folly::Optional<T> value;
};

class SwiftClient {
public:
    SwiftClient(folly::fbstring keystoneUrl, folly::fbstring swiftContainer,
        folly::fbstring username, folly::fbstring password,
        folly::fbstring projectName, folly::fbstring userDomainName,
        folly::fbstring projectDomainName);

    SwiftResult<bool> containerExists();

    SwiftResult<std::size_t> putObject(const folly::fbstring &key,
        folly::IOBufQueue buf, const std::size_t offset);

    SwiftResult<folly::Unit> deleteObject(const folly::fbstring &key);

    SwiftResult<std::vector<SwiftResult<folly::Unit>>> deleteObjects(
        const folly::fbvector<folly::fbstring> &keys);

    SwiftResult<folly::IOBufQueue> getObject(
        const folly::fbstring &key, const off_t offset, const std::size_t size);

private:
    void authenticateIfNeeded();

    folly::fbstring m_keystoneUrl;
    folly::fbstring m_swiftEndpoint; // Retrieved from Keystone service catalog.
    folly::fbstring m_swiftContainer;
    folly::fbstring m_username;
    folly::fbstring m_password;
    folly::fbstring m_projectName;
    folly::fbstring m_userDomainName;
    folly::fbstring m_projectDomainName;

    // Authentication token and its expiry.
    folly::fbstring m_token;
    std::chrono::system_clock::time_point m_tokenExpiry;
};

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

    ~SwiftHelper() override = default;

    folly::fbstring name() const override { return SWIFT_HELPER_NAME; };

    void checkStorageAvailability() override;

    HELPER_PARAM_GETTER(authUrl)

    HELPER_PARAM_GETTER(containerName)

    HELPER_PARAM_GETTER(projectName)

    HELPER_PARAM_GETTER(username)

    HELPER_PARAM_GETTER(password)

    HELPER_PARAM_GETTER(userDomainName)

    HELPER_PARAM_GETTER(projectDomainName)

    bool supportsBatchDelete() const override { return true; }

    folly::IOBufQueue getObject(const folly::fbstring &key, const off_t offset,
        const std::size_t size) override;

    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    void deleteObject(const folly::fbstring &key) override;

    void deleteObjects(const folly::fbvector<folly::fbstring> &keys) override;

private:
    std::unique_ptr<SwiftClient> m_client;

    folly::fbstring m_containerName;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_SWIFT_HELPER_H
