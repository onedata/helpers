/**
 * @file nfsHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include <folly/FBString.h>

#include <chrono>

namespace one {
namespace helpers {

/**
 * @c NFSHelperParams stores the internal helper parameters specific to
 * NFSHelper.
 */
class NFSHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<NFSHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    const folly::fbstring &host() const;

    const boost::filesystem::path &volume() const;

    uid_t uid() const;

    gid_t gid() const;

    size_t readahead() const;

    int tcpSyncnt() const;

    bool dircache() const;

    bool autoreconnect() const;

    int version() const;

private:
    // NFS server host
    folly::fbstring m_host;

    // NFS mount volume
    boost::filesystem::path m_volume;

    // UID value to use when talking to the server.
    // default it 65534 on Windows and getuid() on unixen.
    uid_t m_uid;

    // GID value to use when talking to the server.
    // default it 65534 on Windows and getgid() on unixen.
    gid_t m_gid;

    // Enable readahead for files and set the maximum amount
    // of readahead to <int> bytes.
    size_t m_readahead;

    // Number of SYNs to send during the session establish
    // before failing setting up the tcp connection to the
    // server.
    int m_tcpSyncnt;

    // Disable/enable directory caching. Enabled by default.
    bool m_dircache{true};

    // Control the auto-reconnect behaviour to the NFS session.
    //   -1 : Try to reconnect forever on session failures.
    //        Just like normal NFS clients do.
    //    0 : Disable auto-reconnect completely and immediately
    //        return a failure to the application.
    //  >=1 : Retry to connect back to the server this many
    //        times before failing and returing an error back
    //        to the application.
    int m_autoreconnect;

    // NFS Version. Default is 3.
    int m_version{3};
};
} // namespace helpers
} // namespace one
