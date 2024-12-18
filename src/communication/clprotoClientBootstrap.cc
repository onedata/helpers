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

#include <utility>

namespace one {
namespace communication {

/**
 * List of consecutive socket reconnection delays in milliseconds.
 */
static const std::array<int, CLIENT_RECONNECT_DELAYS_COUNT>
    CLIENT_RECONNECT_DELAYS{0, 10, 10000, 10000, 10000, 10000, 10000, 10000,
        30000, 30000, 30000, 60'000};

static const auto CLIENT_CONNECT_TIMEOUT_SECONDS = 30;

CLProtoClientBootstrap::CLProtoClientBootstrap(const uint32_t id,
    const bool performCLProtoUpgrade, const bool performCLProtoHandshake)
    : m_connectionId{id}
    , m_performCLProtoUpgrade{performCLProtoUpgrade}
    , m_performCLProtoHandshake{performCLProtoHandshake}
{
}

CLProtoClientBootstrap::~CLProtoClientBootstrap() { m_stopping = true; }

void CLProtoClientBootstrap::makePipeline(
    std::shared_ptr<folly::AsyncTransport> socket)
{
    wangle::ClientBootstrap<CLProtoPipeline>::makePipeline(socket);
}

void CLProtoClientBootstrap::setEOFCallback(
    std::function<void(void)> eofCallback)
{
    m_eofCallback = std::move(eofCallback);
}

folly::Future<folly::Unit> CLProtoClientBootstrap::connect(
    const folly::fbstring &host, const int port, size_t reconnectAttempt)
{
    m_firstConnection = false;

    reconnectAttempt =
        std::min<size_t>(CLIENT_RECONNECT_DELAYS.size() - 1, reconnectAttempt);

    auto reconnectDelay = CLIENT_RECONNECT_DELAYS.at(
        std::min(reconnectAttempt, CLIENT_RECONNECT_DELAYS.size()));

    if (reconnectAttempt == 0U) {
        LOG_DBG(2) << "Creating new connection with id " << connectionId()
                   << " to " << host << ":" << port;
    }
    else {
        LOG(INFO) << "Reconnecting connection with id " << connectionId()
                  << " to " << host.toStdString() << ":" << port << " in "
                  << reconnectDelay << " ms. Attempt: " << reconnectAttempt;
    }

    auto *executor = group_.get();

    return folly::makeFuture()
        .via(executor)
        .thenValue([this, reconnectAttempt, reconnectDelay](auto && /*unit*/) {
            if (reconnectAttempt == 0)
                LOG_DBG(1) << "Connect attempt in " << reconnectDelay
                           << " [ms]";
            else
                LOG_DBG(1) << "Reconnect attempt " << reconnectAttempt << " in "
                           << reconnectDelay << " [ms]";

            m_handshakeDone = false;

            // If this is a reconnect attempt and the pipeline still exists,
            // close it first
            if ((reconnectAttempt != 0u) && (getPipeline() != nullptr)) {
                return getPipeline()->close();
            }

            return folly::makeFuture();
        })
        .delayed(std::chrono::milliseconds{reconnectDelay})
        .thenValue([this, host, port, reconnectAttempt, executor](
                       auto && /*unit*/) {
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
                address, std::chrono::seconds{CLIENT_CONNECT_TIMEOUT_SECONDS})
                .thenValue([this, addressStr = address.describe(), host, port,
                               executor](CLProtoPipeline *pipeline) {
                    pipeline->getHandler<codec::CLProtoMessageHandler>()
                        ->setEOFCallback(m_eofCallback);

                    return folly::makeSemiFuture()
                        .via(executor)
                        .thenValue(
                            // After connection, attempt to upgrade the
                            // connection to clproto if required for this
                            // connection
                            [this, pipeline, addressStr, host](
                                auto && /*unit*/) mutable {
                                auto sock = std::dynamic_pointer_cast<
                                    folly::AsyncSocket>(
                                    pipeline->getTransport());

                                sock->setNoDelay(true);

                                if (m_performCLProtoUpgrade) {
                                    LOG_DBG(2)
                                        << "Sending clproto upgrade request to "
                                        << addressStr;

                                    auto upgradeRequest =
                                        pipeline // NOLINT
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

                                return folly::makeFuture();
                            })
                        // Wait for the clproto upgrade response from server
                        .thenValue([this, pipeline](auto && /*unit*/) {
                            if (m_performCLProtoUpgrade) {
                                LOG_DBG(3) << "CLProtoUpgradeResponseHandler - "
                                              "waiting for response";

                                return pipeline
                                    ->getHandler<
                                        codec::CLProtoUpgradeResponseHandler>()
                                    ->done();
                            }

                            return folly::makeFuture();
                        })
                        // Once upgrade is finished successfully, remove the
                        // clproto upgrade handler
                        .thenValue([this, pipeline](auto && /*unit*/) {
                            if (m_performCLProtoUpgrade) {
                                LOG_DBG(3)
                                    << "Removing clproto upgrade handler";

                                pipeline->remove<
                                    codec::CLProtoUpgradeResponseHandler>();
                            }
                            pipeline->finalize();
                        })
                        // If CLProto handshake is provided, perform handshake
                        // before handling any other requests
                        .thenValue([this, pipeline](auto && /*unit*/) {
                            if (m_performCLProtoHandshake) {
                                auto *handshakeHandler = pipeline->getHandler<
                                    codec::CLProtoHandshakeResponseHandler>();

                                auto handshake =
                                    handshakeHandler->getHandshake();

                                LOG_DBG(2)
                                    << "Sending handshake message (length "
                                       "= "
                                    << handshake.size() << "): " << handshake;

                                return pipeline->write(std::move(handshake));
                            }

                            return folly::makeFuture();
                        })
                        .thenValue([this, pipeline](auto && /*unit*/) {
                            if (m_performCLProtoHandshake) {
                                LOG_DBG(2)
                                    << "Handshake sent - waiting for reply";

                                return pipeline
                                    ->getHandler<codec::
                                            CLProtoHandshakeResponseHandler>()
                                    ->done();
                            }

                            return folly::makeFuture();
                        })
                        // Once upgrade is finished, remove the
                        // clproto upgrade handler
                        .thenTry([this, pipeline, host, port](
                                     auto &&maybeUnit) {
                            if (m_performCLProtoHandshake) {
                                if (maybeUnit.hasException()) {
                                    if (pipeline != nullptr)
                                        pipeline->finalize();
                                    LOG(ERROR)
                                        << "Connection refused by remote "
                                           "Oneprovider at "
                                        << host << ":" << port << ": "
                                        << folly::exceptionStr(
                                               maybeUnit.exception());

                                    maybeUnit.throwIfFailed();
                                }

                                LOG_DBG(3)
                                    << "Removing clproto handshake handler";

                                if (pipeline != nullptr) {
                                    pipeline->remove<codec::
                                            CLProtoHandshakeResponseHandler>();
                                    pipeline->finalize();
                                }

                                m_handshakeDone = true;
                                LOG_DBG(1) << "CLProto connection with id "
                                           << connectionId() << " established";
                            }
                        });
                })
                .thenError(folly::tag_t<std::system_error>{},
                    [this, host, port, executor, reconnectAttempt](auto &&e) {
                        LOG(ERROR) << "Reconnect attempt failed: " << e.what();

                        auto err = e.code().value();

                        if (err == EAGAIN || err == ECONNRESET ||
                            err == ECONNABORTED || err == ECONNREFUSED) {

                            // onError() doesn't keep the executor, so we have
                            // to wrap it in via
                            return folly::makeFuture().via(executor).thenValue(
                                [this, host, port, reconnectAttempt](
                                    auto && /*unit*/) {
                                    return connect(
                                        host, port, reconnectAttempt + 1)
                                        .thenValue([](auto && /*unit*/) {
                                            return folly::makeFuture();
                                        });
                                });
                        }

                        if (getPipeline() != nullptr) {
                            // close() must be called in executor
                            //
                            return folly::makeFuture()
                                .via(executor)
                                .thenValue([this](auto && /*unit*/) {
                                    getPipeline()->close();
                                })
                                .thenValue([e](auto && /*unit*/) { throw e; });
                        }

                        throw e;
                    });
        });
}

bool CLProtoClientBootstrap::connected()
{
    if (m_eofCallbackCalled || m_stopping)
        return false;

    if (m_performCLProtoHandshake && !m_handshakeDone)
        return false;

    if (!pipeline_ || pipeline_.use_count() == 0)
        return false;

    if (m_eofCallbackCalled || m_stopping)
        return false;

    if (getPipeline() == nullptr)
        return false;

    if (m_eofCallbackCalled || m_stopping)
        return false;

    if (getPipeline() != nullptr && !getPipeline()->getTransport())
        return false;

    if (m_eofCallbackCalled || m_stopping)
        return false;

    return getPipeline()->getTransport()->good();
}

void CLProtoClientBootstrap::setEOFCallbackCalled(bool v)
{
    m_eofCallbackCalled = v;
}

bool CLProtoClientBootstrap::handshakeDone() const { return m_handshakeDone; }

uint32_t CLProtoClientBootstrap::connectionId() const { return m_connectionId; }

bool CLProtoClientBootstrap::idle() const { return m_idle; }

void CLProtoClientBootstrap::idle(bool i) { m_idle = i; }

bool CLProtoClientBootstrap::firstConnection() const
{
    return m_firstConnection;
}

} // namespace communication
} // namespace one
