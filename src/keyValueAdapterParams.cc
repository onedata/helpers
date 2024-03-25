/**
 * @file keyValueAdapterParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "keyValueAdapterParams.h"

namespace one {
namespace helpers {

std::shared_ptr<KeyValueAdapterParams> KeyValueAdapterParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<KeyValueAdapterParams>();
    result->initializeFromParams(parameters);
    return result;
}

void KeyValueAdapterParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    m_maxCanonicalObjectSize = getParam<std::size_t>(parameters,
        "maxCanonicalObjectSize", constants::MAX_CANONICAL_OBJECT_SIZE);
}

} // namespace helpers
} // namespace one
