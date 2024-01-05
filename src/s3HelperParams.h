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
 * @c S3Params stores the internal helper parameters specific to
 * CephHelper.
 */
class S3HelperParams : public KeyValueAdapterParams {
public:
    static std::shared_ptr<S3HelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override
    {
        const int kDefaultMaxConnections{25};

        const mode_t kDefaultFileMode = 0664;
        const mode_t kDefaultDirMode = 0775;

        m_scheme = getParam(parameters, "scheme", "https");
        m_hostname = getParam(parameters, "hostname");
        m_bucketName = getParam(parameters, "bucketName");
        m_accessKey = getParam<std::string>(parameters, "accessKey", "");
        m_secretKey = getParam<std::string>(parameters, "secretKey", "");
        m_version = getParam<int>(parameters, "signatureVersion", 4);
        m_verifyServerCertificate =
            getParam<bool>(parameters, "verifyServerCertificate", true);
        m_disableExpectHeader =
            getParam<bool>(parameters, "disableExpectHeader", false);
        m_enableClockSkewAdjustment =
            getParam<bool>(parameters, "enableClockSkewAdjustment", false);
        m_maxConnections =
            getParam<int>(parameters, "maxConnections", kDefaultMaxConnections);
        m_fileMode = getParam<mode_t>(parameters, "fileMode", kDefaultFileMode);
        m_dirMode = getParam<mode_t>(parameters, "dirMode", kDefaultDirMode);
        m_region = getParam<std::string>(parameters, "region", "us-east-1");

        if (m_version != 4)
            throw std::invalid_argument("Unsupported S3 signature version: " +
                std::to_string(m_version) +
                ". Currently only supported signature "
                "version is '4'.");
    }

    const folly::fbstring &scheme() const { return m_scheme; }

    const folly::fbstring &hostname() const { return m_hostname; }

    const folly::fbstring &bucketName() const { return m_bucketName; }

    const folly::fbstring &accessKey() const { return m_accessKey; }

    const folly::fbstring &secretKey() const { return m_secretKey; }

    int version() const { return m_version; }

    bool verifyServerCertificate() const { return m_version; }

    bool disableExpectHeader() const { return m_version; }

    bool enableClockSkewAdjustment() const { return m_version; }

    int maxConnections() const { return m_version; }

    mode_t fileMode() const { return m_fileMode; }

    mode_t dirMode() const { return m_dirMode; }

    const folly::fbstring &region() const { return m_region; }

private:
    folly::fbstring m_scheme;
    folly::fbstring m_hostname;
    folly::fbstring m_bucketName;
    folly::fbstring m_accessKey;
    folly::fbstring m_secretKey;
    int m_version;
    bool m_verifyServerCertificate;
    bool m_disableExpectHeader;
    bool m_enableClockSkewAdjustment;
    int m_maxConnections;
    mode_t m_fileMode;
    mode_t m_dirMode;
    folly::fbstring m_region;
};
} // namespace helpers
} // namespace one
