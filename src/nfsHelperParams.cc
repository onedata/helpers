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

    m_host = getParam(parameters, "host").toStdString();
    m_volume = getParam(parameters, "volume").toStdString();
    m_uid = getParam<uid_t>(parameters, "uid", 0);
    m_gid = getParam<gid_t>(parameters, "gid", 0);
    m_readahead = getParam<size_t>(parameters, "readahead", 0);
    m_tcpSyncnt = getParam<int>(parameters, "tcpSyncnt", 0);
    m_dircache = getParam<bool>(parameters, "dircache", true);
    m_autoreconnect = getParam<int>(parameters, "autoreconenct", 0);
    m_version = getParam<int>(parameters, "version", kNFSDefaultVersion);
}

const folly::fbstring &NFSHelperParams::host() const { return m_host; }

const boost::filesystem::path &NFSHelperParams::volume() const
{
    return m_volume;
}

uid_t NFSHelperParams::uid() const { return m_uid; }

gid_t NFSHelperParams::gid() const { return m_gid; }

size_t NFSHelperParams::readahead() const { return m_readahead; }

int NFSHelperParams::tcpSyncnt() const { return m_tcpSyncnt; }

bool NFSHelperParams::dircache() const { return m_dircache; }

bool NFSHelperParams::autoreconnect() const { return m_autoreconnect; }

int NFSHelperParams::version() const { return m_version; }

} // namespace helpers
} // namespace one
