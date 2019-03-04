/**
 * @file webDAVHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2019 ACK CYFRONET AGH
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

enum class WebDAVCredentialsType { NONE, BASIC, TOKEN, OAUTH2 };

enum class WebDAVRangeWriteSupport {
    NONE,                   // No write support
    SABREDAV_PARTIALUPDATE, // Range write using SabreDAV PATCH extension
    MODDAV_PUTRANGE         // Range write using mod_dav PUT with Content-Range
};

/**
 * @c WebDAVHelperParams stores the internal helper parameters specific to
 * WebDAVHelper.
 */
class WebDAVHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<WebDAVHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    const Poco::URI &endpoint() const;

    bool verifyServerCertificate() const;

    WebDAVCredentialsType credentialsType() const;

    const folly::fbstring &credentials() const;

    const folly::fbstring &authorizationHeader() const;

    const folly::fbstring &oauth2IdP() const;

    const folly::fbstring &accessToken() const;

    std::chrono::seconds accessTokenTTL() const;

    WebDAVRangeWriteSupport rangeWriteSupport() const;

    uint32_t connectionPoolSize() const;

    size_t maximumUploadSize() const;

    std::chrono::system_clock::time_point createdOn() const;

    bool testTokenRefreshMode() const;

private:
    Poco::URI m_endpoint;
    bool m_verifyServerCertificate;
    WebDAVCredentialsType m_credentialsType;
    folly::fbstring m_credentials;
    folly::fbstring m_authorizationHeader;
    folly::fbstring m_oauth2IdP;
    folly::fbstring m_accessToken;
    std::chrono::seconds m_accessTokenTTL;
    WebDAVRangeWriteSupport m_rangeWriteSupport;
    uint32_t m_connectionPoolSize;
    size_t m_maximumUploadSize;
    std::chrono::system_clock::time_point m_createdOn;

    // This is for integration tests only
    bool m_testTokenRefreshMode;
};
} // namespace helpers
} // namespace one
