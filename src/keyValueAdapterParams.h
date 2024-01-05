/**
 * @file keyValueAdapterParams.h
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
class KeyValueAdapterParams : public StorageHelperParams {
public:
    static std::shared_ptr<KeyValueAdapterParams> create(
        const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    /**
     * Return the maximum object size, which can be written or modified on this
     * storage. This is a user defined setting.
     */
    std::size_t maxCanonicalObjectSize() const
    {
        return m_maxCanonicalObjectSize;
    }

    std::size_t blockSize() const { return m_blockSize; }

    StoragePathType storagePathType() const { return m_storagePathType; }

private:
    bool m_randomAccess;
    std::size_t m_maxCanonicalObjectSize;
    std::size_t m_blockSize;
    StoragePathType m_storagePathType;
};
} // namespace helpers
} // namespace one
