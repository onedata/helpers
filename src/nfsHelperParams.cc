/**
 * @file nfsHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nfsHelperParams.h"

namespace one {
namespace helpers {

std::shared_ptr<NFSHelperParams> NFSHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<NFSHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void NFSHelperParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    const auto kNFSDefaultVersion = 3;
    const auto kNFSDefaultConnectionPoolSize = 10;

    m_host = getParam(parameters, "host").toStdString();
    m_volume = getParam(parameters, "volume").toStdString();
    m_uid = getParam<uid_t>(parameters, "uid", 0);
    m_gid = getParam<gid_t>(parameters, "gid", 0);
    m_readAhead = getParam<size_t>(parameters, "readAhead", 0);
    m_tcpSyncnt = getParam<int>(parameters, "tcpSyncnt", 0);
    m_dirCache = getParam<bool>(parameters, "dirCache", true);
    m_autoReconnect = getParam<int>(parameters, "autoReconnect", 1);
    m_version = getParam<int>(parameters, "version", kNFSDefaultVersion);
    m_connectionPoolSize = getParam<int>(
        parameters, "connectionPoolSize", kNFSDefaultConnectionPoolSize);
}

const folly::fbstring &NFSHelperParams::host() const { return m_host; }

const boost::filesystem::path &NFSHelperParams::volume() const
{
    return m_volume;
}

uid_t NFSHelperParams::uid() const { return m_uid; }

gid_t NFSHelperParams::gid() const { return m_gid; }

size_t NFSHelperParams::readAhead() const { return m_readAhead; }

int NFSHelperParams::tcpSyncnt() const { return m_tcpSyncnt; }

bool NFSHelperParams::dirCache() const { return m_dirCache; }

int NFSHelperParams::autoReconnect() const { return m_autoReconnect; }

int NFSHelperParams::version() const { return m_version; }

int NFSHelperParams::connectionPoolSize() const { return m_connectionPoolSize; }

} // namespace helpers
} // namespace one
