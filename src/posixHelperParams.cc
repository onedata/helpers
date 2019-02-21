/**
 * @file posixHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2019 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "posixHelperParams.h"

namespace one {
namespace helpers {

std::shared_ptr<PosixHelperParams> PosixHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<PosixHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void PosixHelperParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    m_mountPoint = getParam(parameters, "mountPoint").toStdString();
    m_uid = getParam<uid_t>(parameters, "uid", -1);
    m_gid = getParam<gid_t>(parameters, "gid", -1);
}

const boost::filesystem::path &PosixHelperParams::mountPoint() const
{
    return m_mountPoint;
}

uid_t PosixHelperParams::uid() const { return m_uid; }

gid_t PosixHelperParams::gid() const { return m_gid; }

} // namespace helpers
} // namespace one
