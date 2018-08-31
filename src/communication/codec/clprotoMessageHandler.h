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

/**
 * @c CLProtoMessageHandler is responsible for running clproto message callbacks
 * after receiving entire messages.
 */
class CLProtoMessageHandler : public wangle::InboundHandler<std::string> {
public:
    CLProtoMessageHandler(std::function<void(std::string)> onMessage)
        : m_onMessage{std::move(onMessage)}
    {
    }

    void setEOFCallback(std::function<void(void)> eofCallback)
    {
        m_eofCallback = eofCallback;
    }

    void read(Context *, std::string msg) override
    {
        m_onMessage(std::move(msg));
    }

    void readException(Context *ctx, folly::exception_wrapper e) override
    {
        LOG(ERROR) << "Unexpected error on clproto socket: "
                   << folly::exceptionStr(e);
    }

    void readEOF(Context *ctx) override
    {
        LOG(ERROR) << "EOF on clproto socket - closing pipeline...";
        if (m_eofCallback)
            m_eofCallback();
    }

private:
    std::function<void(std::string)> m_onMessage;
    std::function<void(void)> m_eofCallback;
};
}
}
}
