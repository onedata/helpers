/**
 * @file glusterfsHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include <chrono>

namespace one {
namespace helpers {

/**
 * @c GlusterFSHelperParams stores the internal helper parameters specific to
 * CephHelper.
 */
class GlusterFSHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<GlusterFSHelperParams> create(
        const Params &parameters);

    void initializeFromParams(const Params &parameters) override
    {
        constexpr int kGlusterFSDefaultPort{24007};

        m_mountPoint = getParam<std::string>(parameters, "mountPoint", "");
        m_uid = getParam<int>(parameters, "uid", -1);
        m_gid = getParam<int>(parameters, "gid", -1);
        m_hostname = getParam<std::string>(parameters, "hostname");
        m_port = getParam<int>(parameters, "port", kGlusterFSDefaultPort);
        m_volume = getParam<std::string>(parameters, "volume");
        m_transport = getParam<std::string>(parameters, "transport", "tcp");
        m_xlatorOptions =
            getParam<std::string>(parameters, "xlatorOptions", "");
    }

    const folly::fbstring &mountPoint() const;
    uid_t uid() const;
    gid_t gid() const;
    const folly::fbstring &hostname() const;
    int port() const;
    const folly::fbstring &volume() const;
    const folly::fbstring &transport() const;
    const folly::fbstring &xlatorOptions() const;

private:
    folly::fbstring m_mountPoint;
    uid_t m_uid;
    gid_t m_gid;
    folly::fbstring m_hostname;
    int m_port;
    folly::fbstring m_volume;
    folly::fbstring m_transport;
    folly::fbstring m_xlatorOptions;
};
} // namespace helpers
} // namespace one
