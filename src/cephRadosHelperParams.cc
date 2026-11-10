/**
 * @file cephRadosHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephRadosHelperParams.h"

namespace one {
namespace helpers {

std::shared_ptr<CephRadosHelperParams> CephRadosHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<CephRadosHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void CephRadosHelperParams::initializeFromParams(const Params &parameters)
{
    KeyValueAdapterParams::initializeFromParams(parameters);

    m_clusterName = getParam(parameters, "clusterName");
    m_monitorHostname = getParam(parameters, "monitorHostname");
    m_poolName = getParam(parameters, "poolName");
    m_username = getParam(parameters, "username");
    m_key = getParam(parameters, "key");
}

const folly::fbstring &CephRadosHelperParams::clusterName() const
{
    return m_clusterName;
}

const folly::fbstring &CephRadosHelperParams::monitorHostname() const
{
    return m_monitorHostname;
}

const folly::fbstring &CephRadosHelperParams::poolName() const
{
    return m_poolName;
}

const folly::fbstring &CephRadosHelperParams::username() const
{
    return m_username;
}

const folly::fbstring &CephRadosHelperParams::key() const { return m_key; }

} // namespace helpers
} // namespace one
