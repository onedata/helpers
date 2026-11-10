/**
 * @file logger.h
 * @author Bartek Kryza
 * @copyright (C) 2022 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_LAYERS_LOGGER_H
#define HELPERS_COMMUNICATION_LAYERS_LOGGER_H

#include "communication/declarations.h"
#include "fuseOperations.h"
#include "helpers/logging.h"

#include <spdlog/sinks/basic_file_sink.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <future>
#include <mutex>
#include <system_error>

namespace one {
namespace communication {
namespace layers {

/**
 * @c Logger is responsible for logging messages into a trace log.
 */
template <class LowerLayer> class Logger : public LowerLayer {
public:
    static const auto kMaxMessageLength = 1024U; // NOLINT

    using Callback = typename LowerLayer::Callback;
    using LowerLayer::LowerLayer;

    Logger() = default;

    virtual ~Logger() = default; // NOLINT

    Logger(const Logger &) = delete;
    Logger(Logger &&) = delete;
    Logger &operator=(const Logger &) = delete;
    Logger &operator=(Logger &&) = delete;

    /**
     * A reference to @c *this typed as a @c Logger.
     */
    Logger<LowerLayer> &logger = *this; // NOLINT

    void enableMessageLog(const std::string &name, const std::string &path);

    void logClientMessage(const clproto::ClientMessage &msg);

    void logServerMessage(const clproto::ServerMessage &msg);

private:
    bool m_enabled{false};
    std::shared_ptr<spdlog::logger> m_logger;
};

template <class LowerLayer>
void Logger<LowerLayer>::enableMessageLog(
    const std::string &name, const std::string &path)
{
    try {
        m_logger = spdlog::basic_logger_mt(name, path);
        m_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] %v");
        m_enabled = true;

        LOG(INFO) << "Started message trace logging to: " << path;
    }
    catch (const spdlog::spdlog_ex &ex) {
        LOG(ERROR) << "Message log init failed: " << ex.what() << std::endl;
    }
}

template <class LowerLayer>
void Logger<LowerLayer>::logClientMessage(const clproto::ClientMessage &msg)
{
    if (m_enabled) {
        if (!msg.has_proxyio_request()) {
            m_logger->info(">>> \n{}---", msg.DebugString());
        }
        else {
            const auto str = msg.DebugString();
            if (str.size() < kMaxMessageLength)
                m_logger->info(">>> \n{}---", str);
            else
                m_logger->info(
                    ">>> \n{}...\n---", str.substr(0, kMaxMessageLength - 1));
        }
        m_logger->flush();
    }
}

template <class LowerLayer>
void Logger<LowerLayer>::logServerMessage(const clproto::ServerMessage &msg)
{
    if (m_enabled) {
        if (!msg.has_proxyio_response()) {
            m_logger->info("<<< \n{}---", msg.DebugString());
        }
        else {
            const auto str = msg.DebugString();
            if (str.size() < kMaxMessageLength)
                m_logger->info("<<< \n{}---", str);
            else
                m_logger->info(
                    "<<< \n{}...\n---", str.substr(0, kMaxMessageLength - 1));
        }

        m_logger->flush();
    }
}

} // namespace layers
} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_LAYERS_LOGGER_H
