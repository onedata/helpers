/**
 * @file nullDeviceHelperParams.h
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include <folly/FBString.h>

#include <chrono>

namespace one {
namespace helpers {

constexpr auto NULL_DEVICE_DEFAULT_SIMULATED_FILE_SIZE = 1024ULL;

/**
 * @c NullDeviceHelperParams stores the internal helper parameters specific to
 * NullDeviceHelper.
 */
class NullDeviceHelperParams : public StorageHelperParams {
public:
    static std::shared_ptr<NullDeviceHelperParams> create(
        const Params &parameters);

    void initializeFromParams(const Params &parameters) override;

    int latencyMin() const;
    int latencyMax() const;
    double timeoutProbability() const;
    const std::vector<std::string> &filter() const;
    const std::vector<std::pair<int64_t, int64_t>> &
    simulatedFilesystemParameters() const;
    double simulatedFilesystemGrowSpeed() const;
    size_t simulatedFileSize() const;
    bool enableDataVerification() const;
    bool applyToAllOperations() const;

    static std::pair<std::vector<std::pair<int64_t, int64_t>>,
        folly::Optional<size_t>>
    parseSimulatedFilesystemParameters(const std::string &params);

private:
    int m_latencyMin;
    int m_latencyMax;
    double m_timeoutProbability;
    std::vector<std::string> m_filter;
    std::vector<std::pair<int64_t, int64_t>> m_simulatedFilesystemParameters;
    double m_simulatedFilesystemGrowSpeed;
    size_t m_simulatedFileSize;
    bool m_enableDataVerification;
    bool m_applyToAllOperations{false};

    boost::filesystem::path m_mountPoint;
    uid_t m_uid;
    gid_t m_gid;
};
} // namespace helpers
} // namespace one
