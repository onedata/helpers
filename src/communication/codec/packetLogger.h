/**
 * @file packetLogger.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/logging.h"

#include <folly/io/Cursor.h>
#include <wangle/channel/Handler.h>

namespace one {
namespace communication {
namespace codec {

/**
 * @c PacketLogger is an optional handler for the communication pipeline
 * which allows logging of incoming and outgoing raw messages.
 */
class PacketLogger : public wangle::HandlerAdapter<folly::IOBufQueue &,
                         std::unique_ptr<folly::IOBuf>> {
public:
    PacketLogger() = default;

    folly::Future<folly::Unit> write(
        Context *ctx, std::unique_ptr<folly::IOBuf> buf) override
    {
        buf->coalesce();

        LOG_DBG(4) << "Sending packet message: "
                   << LOG_ERL_BIN(std::string(
                          reinterpret_cast<const char *>(buf->data()),
                          buf->length()));

        return ctx->fireWrite(std::move(buf));
    }

    void read(Context *ctx, folly::IOBufQueue &buf) override
    {
        buf.gather(buf.chainLength());

        LOG_DBG(4) << "Received packet message: "
                   << LOG_ERL_BIN(std::string(
                          reinterpret_cast<const char *>(buf.front()->data()),
                          buf.front()->length()));

        ctx->fireRead(buf);
    }
};
} // namespace codec
} // namespace communication
} // namespace one
