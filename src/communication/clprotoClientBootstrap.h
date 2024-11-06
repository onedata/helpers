/**
 * @file clprotoClientBootstrap.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "clprotoPipelineFactory.h"
#include "helpers/logging.h"

#include <folly/init/Init.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/SSLOptions.h>
#include <openssl/ssl.h>
#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/codec/ByteToMessageDecoder.h>
#include <wangle/codec/LineBasedFrameDecoder.h>
#include <wangle/codec/StringCodec.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace one {
namespace communication {

constexpr auto CLIENT_RECONNECT_DELAYS_COUNT{12UL};

/**
 * CLProto specific wrapper over wangle's ClientBoostrap
 */
class CLProtoClientBootstrap : public wangle::ClientBootstrap<CLProtoPipeline> {
public:
    CLProtoClientBootstrap(
        uint32_t id, bool performCLProtoUpgrade, bool performCLProtoHandshake);

    CLProtoClientBootstrap(const CLProtoClientBootstrap &) = delete;
    CLProtoClientBootstrap(CLProtoClientBootstrap &&) = delete;
    CLProtoClientBootstrap &operator=(const CLProtoClientBootstrap &) = delete;
    CLProtoClientBootstrap &operator=(CLProtoClientBootstrap &&) = delete;

    ~CLProtoClientBootstrap() override;

    void makePipeline(std::shared_ptr<folly::AsyncTransport> socket) override;

    folly::Future<folly::Unit> connect(
        const folly::fbstring &host, int port, size_t reconnectAttempt = 0);

    bool connected();

    void setEOFCallbackCalled(bool v);

    bool handshakeDone() const;

    void setEOFCallback(std::function<void(void)> eofCallback);

    uint32_t connectionId() const;

    bool idle() const;

    void idle(bool i);

    bool firstConnection() const;

private:
    using wangle::ClientBootstrap<CLProtoPipeline>::connect;

    const uint32_t m_connectionId;
    const bool m_performCLProtoUpgrade;
    const bool m_performCLProtoHandshake;
    bool m_handshakeDone{false};
    bool m_idle{true};
    bool m_firstConnection{true};

    std::function<void(void)> m_eofCallback;
    bool m_eofCallbackCalled{false};
    bool m_stopping{false};
};
} // namespace communication
} // namespace one
