/**
 * @file packetEncoder.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include <folly/io/Cursor.h>
#include <wangle/channel/Handler.h>
#include <wangle/channel/OutputBufferingHandler.h>

namespace one {
namespace communication {
namespace codec {

/**
 * @c PacketEncoder is responsible for encoding messages in packet
 * format, i.e. prepending 4-byte field length to data frames in network
 * byte order.
 */
class PacketEncoder : public wangle::OutboundBytesToBytesHandler {
public:
    explicit PacketEncoder(uint32_t lengthFieldLength = 4)
        : m_lengthFieldLength{lengthFieldLength}
    {
    }

    folly::Future<folly::Unit> write(
        Context *ctx, std::unique_ptr<folly::IOBuf> buf) override
    {
        int32_t length = htonl(buf->length());
        auto packetBuf =
            folly::IOBuf::copyBuffer(reinterpret_cast<uint8_t *>(&length), 4);

        packetBuf->appendChain(std::move(buf));

        return ctx->fireWrite(std::move(packetBuf));
    }

private:
    const uint32_t m_lengthFieldLength;
};
}
}
}
