/**
 * @file cephHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "glusterfsHelperParams.h"

namespace one {
namespace helpers {

std::shared_ptr<GlusterFSHelperParams> GlusterFSHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<GlusterFSHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

const folly::fbstring &GlusterFSHelperParams::mountPoint() const
{
    return m_mountPoint;
}

uid_t GlusterFSHelperParams::uid() const { return m_uid; }

gid_t GlusterFSHelperParams::gid() const { return m_gid; }

const folly::fbstring &GlusterFSHelperParams::hostname() const
{
    return m_hostname;
}

int GlusterFSHelperParams::port() const { return m_port; }

const folly::fbstring &GlusterFSHelperParams::volume() const
{
    return m_volume;
}

const folly::fbstring &GlusterFSHelperParams::transport() const
{
    return m_transport;
}

const folly::fbstring &GlusterFSHelperParams::xlatorOptions() const
{
    return m_xlatorOptions;
}

} // namespace helpers
} // namespace one
