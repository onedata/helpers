/**
 * @file clprotoHandshakeResponseHandler.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include <folly/io/Cursor.h>
#include <wangle/channel/Handler.h>

namespace one {
namespace communication {
namespace codec {

class CLProtoHandshakeResponseHandler
    : public wangle::InboundHandler<std::string> {
public:
    CLProtoHandshakeResponseHandler(std::function<std::string()> getHandshake,
        std::function<std::error_code(std::string)> onHandshakeResponse,
        std::function<void(std::error_code)> onHandshakeDone)
        : m_getHandshake{std::move(getHandshake)}
        , m_onHandshakeResponse{std::move(onHandshakeResponse)}
        , m_onHandshakeDone{onHandshakeDone}
    {
    }

    void read(Context *ctx, std::string message)
    {
        LOG_DBG(1) << "Received clproto handshake response";

        if (m_promise.isFulfilled()) {
            ctx->fireRead(message);
            return;
        }

        m_promise.setValue();
        auto handshakeResponseError = m_onHandshakeResponse(message);
        if (!handshakeResponseError) {
            m_onHandshakeDone(std::error_code{});
        }
        else {
            m_promise.setException(
                folly::make_exception_wrapper<std::runtime_error>(
                    "Error during clproto handshake."));

            m_onHandshakeDone(handshakeResponseError);
        }
        ctx->fireRead(std::move(message));
    }

    folly::Future<folly::Unit> done() { return m_promise.getFuture(); }

    std::string getHandshake() { return m_getHandshake(); }

private:
    folly::Promise<folly::Unit> m_promise;
    std::function<std::string()> m_getHandshake;
    std::function<std::error_code(std::string)> m_onHandshakeResponse;
    std::function<void(std::error_code)> m_onHandshakeDone;
};
}
}
}
