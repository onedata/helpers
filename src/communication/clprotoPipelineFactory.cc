/**
 * @file clprotoPipelineFactory.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "clprotoPipelineFactory.h"

#include "codec/clprotoHandshakeResponseHandler.h"
#include "codec/clprotoMessageHandler.h"
#include "codec/clprotoUpgradeResponseHandler.h"
#include "codec/packetDecoder.h"
#include "codec/packetEncoder.h"
#include "codec/packetLogger.h"

#include <wangle/channel/OutputBufferingHandler.h>
#include <wangle/codec/ByteToMessageDecoder.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>
#include <wangle/codec/LengthFieldPrepender.h>
#include <wangle/codec/StringCodec.h>

#include <utility>
#include <vector>

namespace one {
namespace communication {

CLProtoPipelineFactory::CLProtoPipelineFactory(const bool clprotoUpgrade)
    : m_clprotoUpgrade{clprotoUpgrade}
{
}

void CLProtoPipelineFactory::setOnMessageCallback(
    std::function<void(std::string)> onMessage)
{
    m_onMessage = std::move(onMessage);
}

void CLProtoPipelineFactory::setHandshake(
    std::function<std::string()> getHandshake,
    std::function<std::error_code(std::string)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
{
    m_getHandshake = std::move(getHandshake);
    m_onHandshakeResponse = std::move(onHandshakeResponse);
    m_onHandshakeDone = std::move(onHandshakeDone);
}

CLProtoPipeline::Ptr CLProtoPipelineFactory::newPipeline(
    std::shared_ptr<folly::AsyncTransportWrapper> sock)
{
    auto pipeline = CLProtoPipeline::create();

    pipeline->addBack(wangle::AsyncSocketHandler{sock});

    pipeline->addBack(wangle::OutputBufferingHandler{});

    pipeline->addBack(wangle::EventBaseHandler{});

    if (VLOG_IS_ON(4))
        pipeline->addBack(codec::PacketLogger{});

    if (m_clprotoUpgrade)
        pipeline->addBack(codec::CLProtoUpgradeResponseHandler{});

    pipeline->addBack(codec::PacketDecoder{});

    pipeline->addBack(codec::PacketEncoder{});

    pipeline->addBack(wangle::StringCodec{});

    if (m_getHandshake)
        pipeline->addBack(codec::CLProtoHandshakeResponseHandler{
            m_getHandshake, m_onHandshakeResponse, m_onHandshakeDone});

    pipeline->addBack(codec::CLProtoMessageHandler{m_onMessage});

    pipeline->finalize();

    pipeline->setTransport(sock);

    return pipeline;
}
} // namespace communication
} // namespace one
