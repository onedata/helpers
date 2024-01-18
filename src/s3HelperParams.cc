/**
 * @file s3HelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "s3HelperParams.h"

namespace one {
namespace helpers {
std::shared_ptr<S3HelperParams> S3HelperParams::create(const Params &parameters)
{
    auto result = std::make_shared<S3HelperParams>();
    result->initializeFromParams(parameters);
    return result;
}
} // namespace helpers
} // namespace one