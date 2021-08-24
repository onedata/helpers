/**
 * @file clprotoUpgradeResponseHandler.h
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

/**
 * @c CLProtoUpgradeResponseHandler is responsible for reading the inbound data
 * stream on the socket after the clproto upgrade has been sent until the HTTP
 * response header is returned from the server.
 */
class CLProtoUpgradeResponseHandler
    : public wangle::HandlerAdapter<folly::IOBufQueue &,
          std::unique_ptr<folly::IOBuf>> {
public:
    CLProtoUpgradeResponseHandler() = default;

    void read(Context *ctx, folly::IOBufQueue &buf) override
    {
        const std::string CLPROTO_UPGRADE_RESPONSE_STATUS{
            "HTTP/1.1 101 Switching Protocols"};

        if (m_promise.isFulfilled()) {
            ctx->fireRead(buf);
            return;
        }

        // Read until end of HTTP header response (i.e. "\r\n\r\n")
        buf.gather(buf.chainLength());
        folly::StringPiece bufStringView(
            reinterpret_cast<const char *>(buf.front()->data()),
            buf.front()->length());

        if (bufStringView.find("\r\n\r\n") == folly::StringPiece::npos) {
            LOG_DBG(1) << "More data expected in HTTP response from server for "
                          "clproto upgrade response...";
            return;
        }

        if (!(bufStringView.find(CLPROTO_UPGRADE_RESPONSE_STATUS) == 0U)) {
            LOG(ERROR) << "Invalid response during clproto protocol upgrade: "
                       << bufStringView << ". Expected:\n '"
                       << CLPROTO_UPGRADE_RESPONSE_STATUS << "'";

            m_promise.setException(
                folly::make_exception_wrapper<std::runtime_error>(
                    "Invalid response from server during clproto upgrade."));
        }
        else {
            LOG_DBG(3) << "Received valid clproto response: " << bufStringView;
            LOG_DBG(2) << "Switching socket protocol to clproto";

            // Clear the buffer for consecutive clproto messages
            buf.clear();
            m_promise.setValue();
            ctx->fireRead(buf);
        }
    }

    folly::Future<folly::Unit> done() { return m_promise.getFuture(); }

    std::unique_ptr<folly::IOBuf> makeUpgradeRequest(std::string host)
    {
        const std::string CLPROTO_UPGRADE_ENDPOINT{"/clproto"};
        std::string request;
        request += "GET " + CLPROTO_UPGRADE_ENDPOINT + " HTTP/1.1\r\n";
        request += "Host: " + host + "\r\n";
        request += "Connection: upgrade\r\n";
        request += "Upgrade: clproto\r\n\r\n";

        auto upgradeRequestBuf =
            folly::IOBuf::copyBuffer(request.data(), request.size());

        LOG_DBG(3) << "Sending clproto request: " << request;

        return upgradeRequestBuf;
    }

private:
    folly::Promise<folly::Unit> m_promise;
};
} // namespace codec
} // namespace communication
} // namespace one
