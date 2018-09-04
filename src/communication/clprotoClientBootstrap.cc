/**
 * @file clprotoClientBootstrap.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "clprotoClientBootstrap.h"

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

namespace one {
namespace communication {

/**
 * List of consecutive socket reconnection delays in milliseconds.
 */
static const std::vector<int> CLIENT_RECONNECT_DELAYS{0, 100, 1000, 1000, 1000,
    1000, 5000, 5000, 5000, 15'000, 15'000, 15'000, 60'000};

static const auto CLIENT_CONNECT_TIMEOUT = std::chrono::seconds{10};

CLProtoClientBootstrap::CLProtoClientBootstrap(const uint32_t id,
    const bool performCLProtoUpgrade, const bool performCLProtoHandshake)
    : m_connectionId{id}
    , m_performCLProtoUpgrade{performCLProtoUpgrade}
    , m_performCLProtoHandshake{performCLProtoHandshake}
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
    const folly::fbstring &host, const int port, size_t reconnectAttempt)
{
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

    auto executor = group_.get();

    assert(executor);

    return folly::via(executor)
        .then(executor,
            [this, reconnectAttempt] {
                // If this is a reconnect attempt and the pipeline still exists,
                // close it first
                if (reconnectAttempt && getPipeline())
                    return getPipeline()->close();
                else
                    return folly::makeFuture();
            })
        .delayed(std::chrono::milliseconds{reconnectDelay})
        .then(executor, [this, host, port, reconnectAttempt, executor] {
            // Initialize SocketAddress from host and port provided on the
            // command line. If necessary, DNS lookup will be performed...
            folly::SocketAddress address;
            try {
                address.setFromHostPort(host.c_str(), port);
            }
            catch (std::system_error &e) {
                LOG(ERROR) << "Cannot resolve host address: " << host;
                throw;
            }

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
                                if (m_performCLProtoUpgrade) {
                                    LOG_DBG(3)
                                        << "Removing clproto upgrade handler";

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
                        });
                })
                .onError([this, host, port, executor, reconnectAttempt](
                             folly::exception_wrapper ew) {
                    if (!reconnectAttempt) {
                        LOG(ERROR) << "Failed connecting to Oneprovider at "
                                   << host << ":" << port;
                        ew.throw_exception();
                    }
                    else {
                        LOG_DBG(1) << "Reconnect attempt failed: " << ew.what()
                                   << ". Retrying...";

                        // onError() doesn't keep the executor, so we have to
                        // wrap it in via
                        return folly::via(
                            executor, [this, host, port, reconnectAttempt] {
                                return connect(host, port, reconnectAttempt + 1)
                                    .then([]() { return folly::makeFuture(); });
                            });
                    }
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
}
}
