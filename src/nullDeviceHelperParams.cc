/**
 * @file nullDeviceHelperParams.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nullDeviceHelperParams.h"

#include <folly/String.h>

namespace one {
namespace helpers {

std::shared_ptr<NullDeviceHelperParams> NullDeviceHelperParams::create(
    const Params &parameters)
{
    auto result = std::make_shared<NullDeviceHelperParams>();
    result->initializeFromParams(parameters);
    return result;
}

void NullDeviceHelperParams::initializeFromParams(const Params &parameters)
{
    StorageHelperParams::initializeFromParams(parameters);

    m_latencyMin = getParam<int>(parameters, "latencyMin", 0);
    m_latencyMax = getParam<int>(parameters, "latencyMax", 0);
    m_timeoutProbability =
        getParam<double>(parameters, "timeoutProbability", 0.0);

    const auto &simulatedFilesystemParameters =
        getParam<folly::fbstring, folly::fbstring>(
            parameters, "simulatedFilesystemParameters", "");
    m_simulatedFilesystemGrowSpeed =
        getParam<double>(parameters, "simulatedFilesystemGrowSpeed", 0.0);

    const auto simulatedFilesystemParametersParsed =
        parseSimulatedFilesystemParameters(
            simulatedFilesystemParameters.toStdString());
    m_simulatedFilesystemParameters =
        std::get<0>(simulatedFilesystemParametersParsed);
    m_simulatedFileSize =
        std::get<1>(simulatedFilesystemParametersParsed)
            .value_or(NULL_DEVICE_DEFAULT_SIMULATED_FILE_SIZE);

    m_enableDataVerification =
        getParam<bool>(parameters, "enableDataVerification", false);

    const auto &filter =
        getParam<folly::fbstring, folly::fbstring>(parameters, "filter", "*");
    if (filter.empty() || filter == "*") {
        m_applyToAllOperations = true;
    }
    else {
        m_applyToAllOperations = false;
        folly::split(",", filter, m_filter, true);
        std::for_each(m_filter.begin(), m_filter.end(), [](auto &token) {
            boost::algorithm::trim(token);
            boost::algorithm::to_lower(token);
        });
    }
}

int NullDeviceHelperParams::latencyMin() const { return m_latencyMin; }

int NullDeviceHelperParams::latencyMax() const { return m_latencyMax; }

double NullDeviceHelperParams::timeoutProbability() const
{
    return m_timeoutProbability;
}

const std::vector<std::string> &NullDeviceHelperParams::filter() const
{
    return m_filter;
}

const std::vector<std::pair<int64_t, int64_t>> &
NullDeviceHelperParams::simulatedFilesystemParameters() const
{
    return m_simulatedFilesystemParameters;
}

double NullDeviceHelperParams::simulatedFilesystemGrowSpeed() const
{
    return m_simulatedFilesystemGrowSpeed;
}

size_t NullDeviceHelperParams::simulatedFileSize() const
{
    return m_simulatedFileSize;
}

bool NullDeviceHelperParams::enableDataVerification() const
{
    return m_enableDataVerification;
}

bool NullDeviceHelperParams::applyToAllOperations() const
{
    return m_applyToAllOperations;
}

std::pair<std::vector<std::pair<int64_t, int64_t>>, folly::Optional<size_t>>
NullDeviceHelperParams::parseSimulatedFilesystemParameters(
    const std::string &params)
{
    auto result = std::vector<std::pair<int64_t, int64_t>>{};

    folly::Optional<size_t> fileSize{};
    if (params.empty())
        return {result, fileSize};

    auto levels = std::vector<std::string>{};

    folly::split(":", params, levels, true);

    auto i = 0U;

    for (const auto &level : levels) {
        auto levelParams = std::vector<std::string>{};
        folly::split("-", level, levelParams, true);

        if (levelParams.size() != 2) {
            if ((i == levels.size() - 1) && (levelParams.size() == 1)) {
                fileSize.emplace(std::stoull(levelParams[0]));
            }
            else {
                throw std::invalid_argument("Invalid null helper simulated "
                                            "filesystem parameters "
                                            "specification: '" +
                    params + "'");
            }
        }
        else
            result.emplace_back(
                std::stoll(levelParams[0]), std::stoll(levelParams[1]));

        i++;
    }
    return {result, fileSize};
}

} // namespace helpers
} // namespace one