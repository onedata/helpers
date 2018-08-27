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

#include <vector>

namespace one {
namespace communication {

/**
 * List of consecutive socket reconnection delays in milliseconds.
 */
static const std::vector<int> CLIENT_RECONNECT_DELAYS{0, 100, 1000, 1000, 1000,
    1000, 5000, 5000, 5000, 15'000, 15'000, 15'000, 60'000};

static const auto CLIENT_CONNECT_TIMEOUT = std::chrono::seconds{20};

CLProtoClientBootstrap::CLProtoClientBootstrap(const uint32_t id,
    const bool performCLProtoUpgrade, const bool performCLProtoHandshake)
    : m_connectionId{id}
    , m_performCLProtoUpgrade{performCLProtoUpgrade}
    , m_performCLProtoHandshake{performCLProtoHandshake}
    , m_reconnectAttempt{0}
{
}

void CLProtoClientBootstrap::makePipeline(
    std::shared_ptr<folly::AsyncSocket> socket)
{
    wangle::ClientBootstrap<CLProtoPipeline>::makePipeline(socket);
}

void CLProtoClientBootstrap::setEOFCallback(
    std::function<void(void)> eofCallback)
{
    m_eofCallback = eofCallback;
}

folly::Future<folly::Unit> CLProtoClientBootstrap::connect(
    const folly::fbstring &host, const int port)
{
    auto reconnectAttempt = m_reconnectAttempt++;

    auto reconnectDelay = CLIENT_RECONNECT_DELAYS.at(
        std::min(reconnectAttempt, CLIENT_RECONNECT_DELAYS.size()));

    if (!reconnectAttempt) {
        LOG_DBG(1) << "Creating new connection with id " << connectionId()
                   << " to " << host.toStdString() << ":" << port;
    }
    else {
        LOG_DBG(1) << "Reconnecting connection with id " << connectionId()
                   << " to " << host.toStdString() << ":" << port << " in "
                   << reconnectDelay << " ms. Attempt: " << reconnectAttempt;
    }

    return folly::via(group_.get())
        .then(group_.get(),
            [this, reconnectAttempt] {
                if (reconnectAttempt && getPipeline())
                    return getPipeline()->close();
                else
                    return folly::makeFuture();
            })
        .delayed(std::chrono::milliseconds{reconnectDelay})
        .then(group_.get(), [this, host, port] {
            // Initialize SocketAddres from host and port provided on the
            // command line. If necessary, DNS lookup will be performed...
            folly::SocketAddress address;
            address.setFromHostPort(host.c_str(), port);

            auto executor = group_.get();

            return wangle::ClientBootstrap<CLProtoPipeline>::connect(
                address, CLIENT_CONNECT_TIMEOUT)
                .then([
                    this, addressStr = address.describe(), host, port, executor
                ](CLProtoPipeline * pipeline) {
                    assert(pipeline);
                    assert(pipeline->getTransport());
                    assert(pipeline->getTransport()->good());

                    pipeline->getHandler<codec::CLProtoMessageHandler>()
                        ->setEOFCallback(m_eofCallback);

                    return via(executor,
                        // After connection, attempt to upgrade the connection
                        // to clproto if required for this connection
                        [this, pipeline, addressStr, host] {
                            auto sock =
                                std::dynamic_pointer_cast<folly::AsyncSocket>(
                                    pipeline->getTransport());

                            assert(sock);

                            sock->setNoDelay(true);

                            if (m_performCLProtoUpgrade) {
                                LOG_DBG(2)
                                    << "Sending clproto upgrade request to "
                                    << addressStr;

                                auto upgradeRequest =
                                    pipeline
                                        ->getHandler<codec::
                                                CLProtoUpgradeResponseHandler>()
                                        ->makeUpgradeRequest(
                                            host.toStdString());

                                // CLProto upgrade request must go over raw
                                // socket without length field prepended
                                return pipeline
                                    ->getContext<wangle::EventBaseHandler>()
                                    ->fireWrite(std::move(upgradeRequest));
                            }
                            else
                                return folly::makeFuture();
                        })
                        // Wait for the clproto upgrade response from server
                        .then(executor,
                            [this, pipeline] {
                                if (m_performCLProtoUpgrade) {
                                    LOG_DBG(3)
                                        << "CLProtoUpgradeResponseHandler - "
                                           "waiting for response";

                                    return pipeline
                                        ->getHandler<codec::
                                                CLProtoUpgradeResponseHandler>()
                                        ->done();
                                }
                                else
                                    return folly::makeFuture();
                            })
                        // Once upgrade is finished successfully, remove the
                        // clproto upgrade handler
                        .then(executor,
                            [this, pipeline] {
                                LOG_DBG(3)
                                    << "Removing clproto upgrade handler";

                                if (m_performCLProtoUpgrade) {
                                    pipeline->remove<
                                        codec::CLProtoUpgradeResponseHandler>();
                                }
                                pipeline->finalize();
                            })
                        // If CLProto handshake is provider, perform handshake
                        // before handling any other requests
                        .then(executor,
                            [this, pipeline] {
                                if (m_performCLProtoHandshake) {
                                    auto handshakeHandler = pipeline->getHandler<
                                        codec::
                                            CLProtoHandshakeResponseHandler>();

                                    assert(handshakeHandler);

                                    auto handshake =
                                        handshakeHandler->getHandshake();

                                    LOG_DBG(3)
                                        << "Sending handshake message (length "
                                           "= "
                                        << handshake.size()
                                        << "): " << handshake;

                                    return pipeline->write(
                                        std::move(handshake));
                                }
                                else
                                    return folly::makeFuture();
                            })
                        .then(executor,
                            [this, pipeline] {
                                if (m_performCLProtoHandshake) {
                                    LOG_DBG(3)
                                        << "Handshake sent - waiting for reply";

                                    return pipeline
                                        ->getHandler<codec::
                                                CLProtoHandshakeResponseHandler>()
                                        ->done();
                                }
                                else
                                    return folly::makeFuture();
                            })
                        // Once upgrade is finished successfully, remove the
                        // clproto upgrade handler
                        .then(executor, [this, pipeline] {
                            if (m_performCLProtoHandshake) {
                                LOG_DBG(3)
                                    << "Removing clproto handshake handler";

                                pipeline->remove<
                                    codec::CLProtoHandshakeResponseHandler>();
                                pipeline->finalize();
                            }

                            LOG_DBG(1) << "CLProto connection with id "
                                       << connectionId() << " established";

                            // Reset the reconnect attempt counter
                            m_reconnectAttempt = 1;
                        });
                })
                .onError(
                    [this, host, port, executor](folly::exception_wrapper ew) {
                        LOG_DBG(1) << "Reconnect attempt failed: " << ew.what()
                                   << ". Retrying...";

                        // onError() doesn't keep the executor, so we have to
                        // wrap it in via
                        return folly::via(executor, [this, host, port] {
                            return connect(host, port).then([]() {
                                return folly::makeFuture();
                            });
                        });
                    });
        });
}

bool CLProtoClientBootstrap::connected()
{
    if (!getPipeline())
        return false;

    if (!getPipeline()->getTransport())
        return false;

    return getPipeline()->getTransport()->good();
}

uint32_t CLProtoClientBootstrap::connectionId() const { return m_connectionId; }

CLProtoPipelineFactory::CLProtoPipelineFactory(const bool clprotoUpgrade)
    : m_clprotoUpgrade{clprotoUpgrade}
{
}

void CLProtoPipelineFactory::setOnMessageCallback(
    std::function<void(std::string)> onMessage)
{
    m_onMessage = onMessage;
}

void CLProtoPipelineFactory::setHandshake(
    std::function<std::string()> getHandshake,
    std::function<std::error_code(std::string)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
{
    m_getHandshake = getHandshake;
    m_onHandshakeResponse = onHandshakeResponse;
    m_onHandshakeDone = onHandshakeDone;
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
}
}
