/**
 * @file cephHelperParams.h
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
 * @c CephHelperParams stores the internal helper parameters specific to
 * CephHelper.
 */
class CephHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<CephHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    const folly::fbstring &clusterName() const;
    const folly::fbstring &monitorHostname() const;
    const folly::fbstring &poolName() const;
    const folly::fbstring &username() const;
    const folly::fbstring &key() const;

private:
    folly::fbstring m_clusterName;
    folly::fbstring m_monitorHostname;
    folly::fbstring m_poolName;
    folly::fbstring m_username;
    folly::fbstring m_key;
};
} // namespace helpers
} // namespace one
