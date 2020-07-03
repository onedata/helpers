/**
 * @file xrootdHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include <XrdCl/XrdClURL.hh>
#include <folly/FBString.h>

#include <chrono>

namespace one {
namespace helpers {

enum class XRootDCredentialsType { NONE, PWD };

/**
 * @c XRootDHelperParams stores the internal helper parameters specific to
 * XRootDHelper.
 */
class XRootDHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<XRootDHelperParams> create(const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    const XrdCl::URL &url() const;

    XRootDCredentialsType credentialsType() const;

    const folly::fbstring &credentials() const;

    mode_t fileModeMask() const;

    mode_t dirModeMask() const;

private:
    XrdCl::URL m_url;
    XRootDCredentialsType m_credentialsType;
    folly::fbstring m_credentials;
    mode_t m_fileModeMask;
    mode_t m_dirModeMask;
};
} // namespace helpers
} // namespace one
