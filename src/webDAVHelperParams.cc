/**
 * @file webDAVHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2019 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "webDAVHelperParams.h"

#include <Poco/Exception.h>

namespace one {
namespace helpers {

std::shared_ptr<WebDAVHelperParams> WebDAVHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<WebDAVHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void WebDAVHelperParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    constexpr auto kDefaultAuthorizationHeader = "Authorization: Bearer {}";
    constexpr auto kDefaultConnectionPoolSize = 10u;
    constexpr auto kDefaultMaximumPoolSize = 0u;
    constexpr auto kDefaultAccessTokenTTL = 0u;

    const auto &endpoint = getParam(parameters, "endpoint");
    const auto &verifyServerCertificateStr =
        getParam(parameters, "verifyServerCertificate", "true");
    const auto &credentialsTypeStr =
        getParam(parameters, "credentialsType", "basic");
    const auto &credentials = getParam(parameters, "credentials", "");
    auto authorizationHeader = getParam<std::string>(
        parameters, "authorizationHeader", kDefaultAuthorizationHeader);
    auto oauth2IdP = getParam<std::string>(parameters, "oauth2IdP", "");
    auto accessToken = getParam<std::string>(parameters, "accessToken", "");
    auto accessTokenTTL = getParam<uint64_t>(
        parameters, "accessTokenTTL", kDefaultAccessTokenTTL);
    const auto &rangeWriteSupportStr =
        getParam(parameters, "rangeWriteSupport", "none");
    const auto connectionPoolSize = getParam<uint32_t>(
        parameters, "connectionPoolSize", kDefaultConnectionPoolSize);
    const auto maximumUploadSize = getParam<size_t>(
        parameters, "maximumUploadSize", kDefaultMaximumPoolSize);
    const auto fileMode = getParam(parameters, "fileMode", "0644");
    const auto dirMode = getParam(parameters, "dirMode", "0775");

    if (authorizationHeader.empty())
        authorizationHeader = kDefaultAuthorizationHeader;

    LOG_FCALL() << LOG_FARG(endpoint) << LOG_FARG(verifyServerCertificateStr)
                << LOG_FARG(credentials) << LOG_FARG(credentialsTypeStr)
                << LOG_FARG(authorizationHeader) << LOG_FARG(accessTokenTTL)
                << LOG_FARG(rangeWriteSupportStr)
                << LOG_FARG(connectionPoolSize) << LOG_FARG(maximumUploadSize);

    Poco::URI endpointUrl;

    constexpr auto kHTTPDefaultPort = 80;
    constexpr auto kHTTPSDefaultPort = 443;

    try {
        std::string scheme;

        if (endpoint.find(":") == folly::fbstring::npos) {
            // The endpoint does not contain neither scheme or port
            scheme = "http://";
        }
        else if (endpoint.find("http") != 0) {
            // The endpoint contains port but not a valid HTTP scheme
            if (endpoint.find(":443") == folly::fbstring::npos)
                scheme = "http://";
            else
                scheme = "https://";
        }

        // Remove trailing '/' from endpoint path if exists
        auto normalizedEndpoint = endpoint.toStdString();
        auto endpointIt = normalizedEndpoint.end() - 1;
        if (*endpointIt == '/')
            normalizedEndpoint.erase(endpointIt);

        endpointUrl = scheme + normalizedEndpoint;
    }
    catch (Poco::SyntaxException &e) {
        throw std::invalid_argument(
            "Invalid WebDAV endpoint: " + endpoint.toStdString());
    }

    if (endpointUrl.getHost().empty())
        throw std::invalid_argument(
            "Invalid WebDAV endpoint - missing hostname: " +
            endpoint.toStdString());

    if (endpointUrl.getScheme().empty()) {
        if (endpointUrl.getPort() == 0) {
            endpointUrl.setScheme("http");
            endpointUrl.setPort(kHTTPDefaultPort);
        }
        else if (endpointUrl.getPort() == kHTTPSDefaultPort) {
            endpointUrl.setScheme("https");
        }
        else {
            endpointUrl.setScheme("http");
        }
    }
    else if (endpointUrl.getScheme() != "http" &&
        endpointUrl.getScheme() != "https") {
        throw std::invalid_argument(
            "Invalid WebDAV endpoint - invalid scheme: " +
            endpointUrl.getScheme());
    }

    if (endpointUrl.getPort() == 0) {
        endpointUrl.setPort(endpointUrl.getScheme() == "https"
                ? kHTTPSDefaultPort
                : kHTTPDefaultPort);
    }

    bool verifyServerCertificate{true};
    if (verifyServerCertificateStr != "true")
        verifyServerCertificate = false;

    WebDAVCredentialsType credentialsType;
    if (credentialsTypeStr == "none")
        credentialsType = WebDAVCredentialsType::NONE;
    else if (credentialsTypeStr == "basic")
        credentialsType = WebDAVCredentialsType::BASIC;
    else if (credentialsTypeStr == "token")
        credentialsType = WebDAVCredentialsType::TOKEN;
    else if (credentialsTypeStr == "oauth2")
        credentialsType = WebDAVCredentialsType::OAUTH2;
    else
        throw std::invalid_argument(
            "Invalid credentials type: " + credentialsTypeStr.toStdString());

    WebDAVRangeWriteSupport rangeWriteSupport;
    if (rangeWriteSupportStr.empty() || rangeWriteSupportStr == "none")
        rangeWriteSupport = WebDAVRangeWriteSupport::NONE;
    else if (rangeWriteSupportStr == "sabredav")
        rangeWriteSupport = WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE;
    else if (rangeWriteSupportStr == "moddav")
        rangeWriteSupport = WebDAVRangeWriteSupport::MODDAV_PUTRANGE;
    else
        throw std::invalid_argument("Invalid range write support specified: " +
            rangeWriteSupportStr.toStdString());

    const auto testTokenRefreshMode =
        getParam(parameters, "testTokenRefreshMode", "false");

    m_endpoint = endpointUrl;
    m_verifyServerCertificate = verifyServerCertificate;
    m_credentialsType = credentialsType;
    m_credentials = credentials;
    m_authorizationHeader = authorizationHeader;
    m_oauth2IdP = oauth2IdP;
    m_accessToken = accessToken;
    m_accessTokenTTL = std::chrono::seconds{accessTokenTTL};
    m_rangeWriteSupport = rangeWriteSupport;
    m_connectionPoolSize = connectionPoolSize;
    m_maximumUploadSize = maximumUploadSize;
    m_createdOn = std::chrono::system_clock::now();
    m_testTokenRefreshMode = (testTokenRefreshMode == "true");
    m_fileMode = parsePosixPermissions(fileMode);
    m_dirMode = parsePosixPermissions(dirMode);
}

const Poco::URI &WebDAVHelperParams::endpoint() const { return m_endpoint; }

bool WebDAVHelperParams::verifyServerCertificate() const
{
    return m_verifyServerCertificate;
}
WebDAVCredentialsType WebDAVHelperParams::credentialsType() const
{
    return m_credentialsType;
}

const folly::fbstring &WebDAVHelperParams::credentials() const
{
    return m_credentials;
}

const folly::fbstring &WebDAVHelperParams::authorizationHeader() const
{
    return m_authorizationHeader;
}

const folly::fbstring &WebDAVHelperParams::oauth2IdP() const
{
    return m_oauth2IdP;
}

const folly::fbstring &WebDAVHelperParams::accessToken() const
{
    return m_accessToken;
}

std::chrono::seconds WebDAVHelperParams::accessTokenTTL() const
{
    return m_accessTokenTTL;
}

WebDAVRangeWriteSupport WebDAVHelperParams::rangeWriteSupport() const
{
    return m_rangeWriteSupport;
}

uint32_t WebDAVHelperParams::connectionPoolSize() const
{
    return m_connectionPoolSize;
}

size_t WebDAVHelperParams::maximumUploadSize() const
{
    return m_maximumUploadSize;
}

std::chrono::system_clock::time_point WebDAVHelperParams::createdOn() const
{
    return m_createdOn;
}

bool WebDAVHelperParams::testTokenRefreshMode() const
{
    return m_testTokenRefreshMode;
}

mode_t WebDAVHelperParams::fileMode() const { return m_fileMode; }

mode_t WebDAVHelperParams::dirMode() const { return m_dirMode; }
} // namespace helpers
} // namespace one
