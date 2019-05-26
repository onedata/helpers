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

/**
 * CLProto specific wrapper over wangle's ClientBoostrap
 */
class CLProtoClientBootstrap : public wangle::ClientBootstrap<CLProtoPipeline> {
public:
    CLProtoClientBootstrap(
        uint32_t id, bool performCLProtoUpgrade, bool performCLProtoHandshake);

    void makePipeline(std::shared_ptr<folly::AsyncSocket> socket) override;

    folly::Future<folly::Unit> connect(
        const folly::fbstring &host, int port, size_t reconnectAttempt = 0);

    bool connected();

    void setEOFCallback(std::function<void(void)> eofCallback);

    uint32_t connectionId() const;

private:
    using wangle::ClientBootstrap<CLProtoPipeline>::connect;

    const uint32_t m_connectionId;
    const bool m_performCLProtoUpgrade;
    const bool m_performCLProtoHandshake;

    std::function<void(void)> m_eofCallback;
};
} // namespace communication
} // namespace one
