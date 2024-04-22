/**
 * @file cephHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephHelperParams.h"

namespace one {
namespace helpers {

std::shared_ptr<CephHelperParams> CephHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<CephHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void CephHelperParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    m_clusterName = getParam(parameters, "clusterName");
    m_monitorHostname = getParam(parameters, "monitorHostname");
    m_poolName = getParam(parameters, "poolName");
    m_username = getParam(parameters, "username");
    m_key = getParam(parameters, "key");
}

const folly::fbstring &CephHelperParams::clusterName() const
{
    return m_clusterName;
}

const folly::fbstring &CephHelperParams::monitorHostname() const
{
    return m_monitorHostname;
}

const folly::fbstring &CephHelperParams::poolName() const { return m_poolName; }

const folly::fbstring &CephHelperParams::username() const { return m_username; }

const folly::fbstring &CephHelperParams::key() const { return m_key; }

} // namespace helpers
} // namespace one
