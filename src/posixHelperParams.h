/**
 * @file posixHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2019 ACK CYFRONET AGH
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
 * @c PosixHelperParams stores the internal helper parameters specific to
 * PosixHelper.
 */
class PosixHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<PosixHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    const boost::filesystem::path &mountPoint() const;

    uid_t uid() const;

    gid_t gid() const;

private:
    boost::filesystem::path m_mountPoint;
    uid_t m_uid;
    gid_t m_gid;
};
} // namespace helpers
} // namespace one
