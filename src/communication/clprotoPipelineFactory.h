/**
 * @file clprotoPipelineFactory.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "logging.h"

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

using CLProtoPipeline = wangle::Pipeline<folly::IOBufQueue &, std::string>;

/**
 * CLProto specific wrapper over wangle's ClientBoostrap
 */
class CLProtoClientBootstrap : public wangle::ClientBootstrap<CLProtoPipeline> {
public:
    CLProtoClientBootstrap(const uint32_t id, const bool performCLProtoUpgrade,
        const bool performCLProtoHandshake);

    void makePipeline(std::shared_ptr<folly::AsyncSocket> socket) override;

    folly::Future<folly::Unit> connect(
        const folly::fbstring &host, const int port);

    bool connected();

    void setEOFCallback(std::function<void(void)> eofCallback);

    uint32_t connectionId() const;

private:
    const uint32_t m_connectionId;
    const bool m_performCLProtoUpgrade;
    const bool m_performCLProtoHandshake;

    std::function<void(void)> m_eofCallback;

    std::atomic<size_t> m_reconnectAttempt;
};

/**
 * CLProto connection pipeline.
 */
class CLProtoPipelineFactory : public wangle::PipelineFactory<CLProtoPipeline> {
public:
    CLProtoPipelineFactory(const bool clprotoUpgrade = true);

    CLProtoPipeline::Ptr newPipeline(
        std::shared_ptr<folly::AsyncTransportWrapper> sock) override;

    void setOnMessageCallback(std::function<void(std::string)> onMessage);

    void setHandshake(std::function<std::string()> getHandshake,
        std::function<std::error_code(std::string)> onHandshakeResponse,
        std::function<void(std::error_code)> onHandshakeDone);

private:
    const bool m_clprotoUpgrade;
    std::function<void(std::string)> m_onMessage;

    std::function<std::string()> m_getHandshake;
    std::function<std::error_code(std::string)> m_onHandshakeResponse;
    std::function<void(std::error_code)> m_onHandshakeDone;
};
}
}
