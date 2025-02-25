/**
 * @file s3HelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include "keyValueAdapterParams.h"

#include <chrono>

namespace one {
namespace helpers {

/**
 * @c SwiftHelperParams stores the internal helper parameters specific to
 * CephHelper.
 */
class SwiftHelperParams : public KeyValueAdapterParams {
public:
    static std::shared_ptr<SwiftHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override
    {
        KeyValueAdapterParams::initializeFromParams(parameters);

        m_authUrl = getParam(parameters, "authUrl");
        m_containerName = getParam(parameters, "containerName");
        m_projectName = getParam(parameters, "projectName");
        m_username = getParam(parameters, "username");
        m_password = getParam(parameters, "password");
        m_userDomainName = getParam(parameters, "userDomainName");
        m_projectDomainName = getParam(parameters, "projectDomainName");
    }

    const folly::fbstring &authUrl() const { return m_authUrl; }

    const folly::fbstring &containerName() const { return m_containerName; }

    const folly::fbstring &projectName() const { return m_projectName; }

    const folly::fbstring &username() const { return m_username; }

    const folly::fbstring &password() const { return m_password; }

    const folly::fbstring &userDomainName() const { return m_userDomainName; }

    const folly::fbstring &projectDomainName() const
    {
        return m_projectDomainName;
    }

private:
    folly::fbstring m_authUrl;
    folly::fbstring m_containerName;
    folly::fbstring m_projectName;
    folly::fbstring m_userDomainName;
    folly::fbstring m_projectDomainName;
    folly::fbstring m_username;
    folly::fbstring m_password;
};
} // namespace helpers
} // namespace one
