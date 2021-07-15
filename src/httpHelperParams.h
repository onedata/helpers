/**
 * @file httpHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include <Poco/URI.h>
#include <folly/FBString.h>

#include <chrono>

namespace one {
namespace helpers {

enum class HTTPCredentialsType { NONE, BASIC, TOKEN, OAUTH2 };

/**
 * @c HTTPHelperParams stores the internal helper parameters specific to
 * HTTPHelper.
 */
class HTTPHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<HTTPHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    const Poco::URI &endpoint() const;

    bool verifyServerCertificate() const;

    HTTPCredentialsType credentialsType() const;

    const folly::fbstring &credentials() const;

    const folly::fbstring &authorizationHeader() const;

    const folly::fbstring &oauth2IdP() const;

    const folly::fbstring &accessToken() const;

    std::chrono::seconds accessTokenTTL() const;

    uint32_t connectionPoolSize() const;

    uint32_t maxRequestsPerSession() const;

    std::chrono::system_clock::time_point createdOn() const;

    bool testTokenRefreshMode() const;

    mode_t fileMode() const;

    mode_t dirMode() const;

private:
    Poco::URI m_endpoint;
    bool m_verifyServerCertificate;
    HTTPCredentialsType m_credentialsType;
    folly::fbstring m_credentials;
    folly::fbstring m_authorizationHeader;
    folly::fbstring m_oauth2IdP;
    folly::fbstring m_accessToken;
    std::chrono::seconds m_accessTokenTTL;
    uint32_t m_connectionPoolSize;
    uint32_t m_maxRequestsPerSession;
    std::chrono::system_clock::time_point m_createdOn;
    mode_t m_fileMode;
    mode_t m_dirMode;

    // This is for integration tests only
    bool m_testTokenRefreshMode;
};
} // namespace helpers
} // namespace one
