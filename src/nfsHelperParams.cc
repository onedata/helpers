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

    m_host = getParam(parameters, "host").toStdString();
    m_path = getParam(parameters, "path").toStdString();
    m_uid = getParam<uid_t>(parameters, "uid", -1);
    m_gid = getParam<gid_t>(parameters, "gid", -1);
    m_traverseMounts = getParam<bool>(parameters, "traverseMounts", true);
    m_readahead = getParam<size_t>(parameters, "readahead", 0);
    m_tcpSyncnt = getParam<int>(parameters, "tcpSyncnt", 0);
    m_dircache = getParam<bool>(parameters, "dircache", true);
    m_autoreconnect = getParam<int>(parameters, "autoreconenct", 0);
    m_version = getParam<int>(parameters, "version", 3);
}

const folly::fbstring &NFSHelperParams::host() const { return m_host; }

const boost::filesystem::path &NFSHelperParams::path() const { return m_path; }

uid_t NFSHelperParams::uid() const { return m_uid; }

gid_t NFSHelperParams::gid() const { return m_gid; }

bool NFSHelperParams::traverseMounts() const { return m_traverseMounts; }

size_t NFSHelperParams::readahead() const { return m_readahead; }

int NFSHelperParams::tcpSyncnt() const { return m_tcpSyncnt; }

bool NFSHelperParams::dircache() const { return m_dircache; }

bool NFSHelperParams::autoreconnect() const { return m_autoreconnect; }

int NFSHelperParams::version() const { return m_version; }

} // namespace helpers
} // namespace one
