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

    void initializeFromParams(const Params &parameters) override;

    const folly::fbstring &scheme() const { return m_scheme; }

    const folly::fbstring &hostname() const { return m_hostname; }

    const folly::fbstring &bucketName() const { return m_bucketName; }

    const folly::fbstring &accessKey() const { return m_accessKey; }

    const folly::fbstring &secretKey() const { return m_secretKey; }

    int version() const { return m_version; }

    bool verifyServerCertificate() const { return m_verifyServerCertificate; }

    bool disableExpectHeader() const { return m_disableExpectHeader; }

    bool enableClockSkewAdjustment() const
    {
        return m_enableClockSkewAdjustment;
    }

    int maxConnections() const { return m_maxConnections; }

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
