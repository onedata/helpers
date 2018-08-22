/**
 * @file clprotoMessageHandler.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include <wangle/channel/Handler.h>

namespace one {
namespace communication {
namespace codec {

class CLProtoMessageHandler : public wangle::InboundHandler<std::string> {
public:
    CLProtoMessageHandler(std::function<void(std::string)> onMessage)
        : m_onMessage{onMessage}
    {
    }

    void read(Context *, std::string msg) override { m_onMessage(msg); }

    void readException(Context *ctx, folly::exception_wrapper e) override
    {
        LOG(ERROR) << "Unexpected error on clproto socket: "
                   << folly::exceptionStr(e);
    }

    void readEOF(Context *ctx) override
    {
        LOG_DBG(1) << "EOF on clproto socket - closing pipeline...";
    }

private:
    std::function<void(std::string)> m_onMessage;
};
}
}
}
