/**
 * @file swiftHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "swiftHelperParams.h"

namespace one {
namespace helpers {
std::shared_ptr<SwiftHelperParams> SwiftHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<SwiftHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}
} // namespace helpers
} // namespace one