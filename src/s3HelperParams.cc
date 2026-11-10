/**
 * @file s3HelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "s3HelperParams.h"

namespace one {
namespace helpers {
std::shared_ptr<S3HelperParams> S3HelperParams::create(const Params &parameters)
{
    auto result = std::make_shared<S3HelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void S3HelperParams::initializeFromParams(const Params &parameters)
{
    KeyValueAdapterParams::initializeFromParams(parameters);

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
        throw std::invalid_argument(
            "Unsupported S3 signature version: " + std::to_string(m_version) +
            ". Currently only supported signature "
            "version is '4'.");
}

} // namespace helpers
} // namespace one