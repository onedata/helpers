/**
 * @file packetDecoder.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include <folly/io/Cursor.h>
#include <wangle/codec/ByteToMessageDecoder.h>

namespace one {
namespace communication {
namespace codec {

/**
 * @c PacketDecoder is responsible for decoding packet messages from the
 * inbound buffer in the Erlang packet format, i.e. data frames with
 * prepended frame length in the first 4 bytes of the message in network
 * byte order.
 */
class PacketDecoder : public wangle::ByteToByteDecoder {
public:
    explicit PacketDecoder(uint32_t lengthFieldLength = 4)
        : m_lengthFieldLength{lengthFieldLength}
    {
    }

    bool decode(Context *ctx, folly::IOBufQueue &buf,
        std::unique_ptr<folly::IOBuf> &result, size_t &) override
    {
        // Here we always assume that the head of the buffer is at
        // the beggining of the frame length field, i.e. the first 4
        // bytes of the buffer contain message length
        if (buf.chainLength() < m_lengthFieldLength)
            return false;

        folly::io::Cursor cursor(buf.front());
        auto messageLength = cursor.readBE<uint32_t>();

        LOG_DBG(3) << "Receiving packet message of length: " << messageLength;

        if (buf.chainLength() >= messageLength + m_lengthFieldLength) {
            buf.trimStart(m_lengthFieldLength);
            result = buf.split(messageLength);

            std::string data((const char *)result->data(), result->length());
            LOG_DBG(4) << "Received packet message: " << LOG_ERL_BIN(data);
            return true;
        }
        else {
            LOG_DBG(3) << "Waiting for remaining "
                       << messageLength - buf.chainLength()
                       << " bytes of message";
            return false;
        }
    }

private:
    uint32_t m_lengthFieldLength;
};
}
}
}
