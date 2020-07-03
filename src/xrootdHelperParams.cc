/**
 * @file xrootdHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "xrootdHelperParams.h"

#include <Poco/Exception.h>

namespace one {
namespace helpers {

std::shared_ptr<XRootDHelperParams> XRootDHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<XRootDHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void XRootDHelperParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    const auto &url = getParam(parameters, "url");
    const auto &credentialsTypeStr =
        getParam(parameters, "credentialsType", "none");
    const auto &credentials = getParam(parameters, "credentials", "");
    const auto fileModeMask = getParam(parameters, "fileModeMask", "0644");
    const auto dirModeMask = getParam(parameters, "dirModeMask", "0775");

    LOG_FCALL() << LOG_FARG(url) << LOG_FARG(credentials)
                << LOG_FARG(credentialsTypeStr);

    constexpr auto kHTTPDefaultPort = 80;
    constexpr auto kHTTPSDefaultPort = 443;
    constexpr auto kXRootDDefaultPort = 1094;

    XrdCl::URL endpointUrl{url.toStdString()};

    if (!endpointUrl.IsValid()) {
        throw std::invalid_argument(
            "Invalid XRootD endpoint: " + url.toStdString());
    }

    if (endpointUrl.GetHostName().empty())
        throw std::invalid_argument(
            "Invalid XRootD endpoint - missing hostname: " + url.toStdString());

    if (endpointUrl.GetProtocol().empty()) {
        if (endpointUrl.GetPort() == 0) {
            endpointUrl.SetProtocol("root");
            endpointUrl.SetPort(kXRootDDefaultPort);
        }
        else if (endpointUrl.GetPort() == kHTTPSDefaultPort) {
            endpointUrl.SetProtocol("https");
        }
        else {
            endpointUrl.SetProtocol("http");
        }
    }
    else if (endpointUrl.GetProtocol() != "root" &&
        endpointUrl.GetProtocol() != "http" &&
        endpointUrl.GetProtocol() != "https") {
        throw std::invalid_argument(
            "Invalid XRootD endpoint - invalid scheme: " +
            endpointUrl.GetProtocol());
    }

    if (endpointUrl.GetPort() == 0) {
        if (endpointUrl.GetProtocol() == "root")
            endpointUrl.SetPort(kXRootDDefaultPort);
        else if (endpointUrl.GetProtocol() == "http")
            endpointUrl.SetPort(kHTTPDefaultPort);
        else if (endpointUrl.GetProtocol() == "https")
            endpointUrl.SetPort(kHTTPSDefaultPort);
    }

    XRootDCredentialsType credentialsType;
    if (credentialsTypeStr == "none")
        credentialsType = XRootDCredentialsType::NONE;
    else if (credentialsTypeStr == "pwd")
        credentialsType = XRootDCredentialsType::PWD;
    else
        throw std::invalid_argument("Invalid XRootD credentials type: " +
            credentialsTypeStr.toStdString());

    m_url = endpointUrl;
    m_credentialsType = credentialsType;
    m_credentials = credentials;
    m_fileModeMask = parsePosixPermissions(fileModeMask);
    m_dirModeMask = parsePosixPermissions(dirModeMask);
}

const XrdCl::URL &XRootDHelperParams::url() const { return m_url; }

mode_t XRootDHelperParams::fileModeMask() const { return m_fileModeMask; }

mode_t XRootDHelperParams::dirModeMask() const { return m_dirModeMask; }
} // namespace helpers
} // namespace one
