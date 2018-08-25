/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "connectionPool.h"

#include "cert/certificateData.h"
#include "codec/clprotoHandshakeResponseHandler.h"
#include "codec/clprotoMessageHandler.h"
#include "codec/clprotoUpgradeResponseHandler.h"
#include "codec/packetDecoder.h"
#include "codec/packetEncoder.h"
#include "codec/packetLogger.h"
#include "exception.h"

#include <algorithm>
#include <array>
#include <folly/Range.h>
#include <folly/io/IOBuf.h>
#include <iterator>
#include <tuple>

using namespace std::placeholders;
using namespace std::literals::chrono_literals;

namespace one {
namespace communication {

ConnectionPool::ConnectionPool(const std::size_t connectionsNumber,
    const std::size_t workersNumber, std::string host,
    const unsigned short port, const bool verifyServerCertificate,
    const bool clprotoUpgrade)
    : m_connectionsNumber{connectionsNumber}
    , m_workersNumber{workersNumber}
    , m_host{host}
    , m_port{port}
    , m_address{m_host, m_port}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_clprotoUpgrade{clprotoUpgrade}
    , m_connected{false}
    , m_executor{std::make_shared<folly::IOThreadPoolExecutor>(1)}
    , m_client{std::make_shared<CLProtoClientBootstrap>()}
{
    LOG_FCALL() << LOG_FARG(connectionsNumber) << LOG_FARG(host)
                << LOG_FARG(port) << LOG_FARG(verifyServerCertificate);

    m_pipelineFactory =
        std::make_shared<CLProtoPipelineFactory>(m_clprotoUpgrade);
    m_client->group(m_executor);
    m_client->pipelineFactory(m_pipelineFactory);
}

std::string ConnectionPool::makeHandshake()
{
    auto handshake = m_getHandshake();
    return handshake;
}

std::shared_ptr<folly::SSLContext> ConnectionPool::createSSLContext()
{
    auto context =
        std::make_shared<folly::SSLContext>(folly::SSLContext::TLSv1_2);

    context->authenticate(m_verifyServerCertificate, false);

    folly::ssl::setSignatureAlgorithms<folly::ssl::SSLCommonOptions>(*context);

    context->setVerificationOption(m_verifyServerCertificate
            ? folly::SSLContext::SSLVerifyPeerEnum::VERIFY
            : folly::SSLContext::SSLVerifyPeerEnum::NO_VERIFY);

    return context;
}

void ConnectionPool::connect()
{
    LOG_FCALL();

    if (m_connected)
        return;

    m_client->sslContext(createSSLContext());

    std::string host = m_address.getAddressStr();

    m_client->connect(m_address)
        .then([this] {
            auto transport = m_client->getPipeline()->getTransport();

            assert(transport->good());

            return via(m_executor.get(),
                // After connection, attempt to upgrade the connection to
                // clproto if required for this connection
                [this, transport] {
                    auto sock = std::dynamic_pointer_cast<folly::AsyncSocket>(
                        transport);

                    assert(sock);

                    sock->setNoDelay(true);

                    if (m_clprotoUpgrade) {
                        LOG_DBG(2) << "Sending clproto upgrade request to "
                                   << m_address.describe();

                        auto upgradeRequest =
                            m_client->getPipeline()
                                ->getHandler<
                                    codec::CLProtoUpgradeResponseHandler>()
                                ->makeUpgradeRequest(m_host);

                        // CLProto upgrade request must go over raw socket
                        // without length field prepended
                        return m_client->getPipeline()
                            ->getContext<wangle::EventBaseHandler>()
                            ->fireWrite(std::move(upgradeRequest));
                    }
                    else
                        return folly::makeFuture();
                })
                // Wait for the clproto upgrade response from server
                .then(m_executor.get(),
                    [this] {
                        if (m_clprotoUpgrade) {
                            LOG_DBG(3) << "CLProtoUpgradeResponseHandler - "
                                          "waiting for response";

                            return m_client->getPipeline()
                                ->getHandler<
                                    codec::CLProtoUpgradeResponseHandler>()
                                ->done();
                        }
                        else
                            return folly::makeFuture();
                    })
                // Once upgrade is finished successfully, remove the
                // clproto upgrade handler
                .then(m_executor.get(),
                    [this] {
                        LOG_DBG(3) << "Removing clproto upgrade handler";

                        auto pipeline = m_client->getPipeline();
                        if (m_clprotoUpgrade) {
                            pipeline->remove<
                                codec::CLProtoUpgradeResponseHandler>();
                        }
                        pipeline->finalize();
                    })
                // If CLProto handshake is provider, perform handshake
                // before handling any other requests
                .then(m_executor.get(),
                    [this] {
                        if (m_getHandshake) {
                            auto handshake = m_getHandshake();

                            LOG_DBG(3) << "Sending handshake message (length = "
                                       << handshake.size() << ")" << handshake;

                            return m_client->getPipeline()->write(
                                std::move(handshake));
                        }
                        else
                            return folly::makeFuture();
                    })
                .then(m_executor.get(),
                    [this] {
                        if (m_getHandshake) {
                            LOG_DBG(3) << "Handshake sent - waiting for reply";

                            return m_client->getPipeline()
                                ->getHandler<
                                    codec::CLProtoHandshakeResponseHandler>()
                                ->done();
                        }
                        else
                            return folly::makeFuture();
                    })
                // Once upgrade is finished successfully, remove the
                // clproto upgrade handler
                .then(m_executor.get(),
                    [this] {
                        if (m_getHandshake) {
                            LOG_DBG(3) << "Removing clproto handshake handler";

                            auto pipeline = m_client->getPipeline();
                            if (m_getHandshake) {
                                pipeline->remove<
                                    codec::CLProtoHandshakeResponseHandler>();
                            }
                            pipeline->finalize();
                        }
                        m_connected = true;
                    })
                .onError([this](folly::exception_wrapper ew) {
                    stop();
                    return folly::makeFuture<folly::Unit>(std::move(ew));
                });
        })
        .get();
}

void ConnectionPool::setHandshake(std::function<std::string()> getHandshake,
    std::function<std::error_code(std::string)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
{
    LOG_FCALL();
    m_pipelineFactory->setHandshake(
        getHandshake, onHandshakeResponse, onHandshakeDone);
    m_getHandshake = std::move(getHandshake);
    m_onHandshakeResponse = std::move(onHandshakeResponse);
    m_onHandshakeDone = std::move(onHandshakeDone);
}

void ConnectionPool::setOnMessageCallback(
    std::function<void(std::string)> onMessage)
{
    LOG_FCALL();

    m_pipelineFactory->setOnMessageCallback(onMessage);
    m_onMessage = onMessage;
}

void ConnectionPool::setCertificateData(
    std::shared_ptr<cert::CertificateData> certificateData)
{
    LOG_FCALL();

    m_certificateData = std::move(certificateData);
}

void ConnectionPool::send(std::string message, Callback callback, const int)
{
    LOG_FCALL() << LOG_FARG(message.size());

    if (!m_connected) {
        LOG(WARNING)
            << "Connection pool already stopped - cannot send message...";
        return;
    }

    LOG_DBG(3) << "Attempting to send message of size " << message.size();

    if (!m_client->getPipeline()->getTransport()->good()) {
        folly::via(m_executor.get(), [c = std::move(callback)]() {
            c(std::make_error_code(std::errc::connection_aborted));
        });
        return;
    }

    m_client->getPipeline()
        ->write(std::move(message))
        .then(m_executor.get(),
            [c = std::move(callback)] { c(std::error_code{}); })
        .get();

    LOG_DBG(3) << "Message sent";
}

ConnectionPool::~ConnectionPool()
{
    LOG_FCALL();

    stop();
}

void ConnectionPool::stop()
{
    LOG_FCALL();

    if (!m_connected)
        return;

    if (!m_client->getPipeline())
        return;

    m_connected = false;

    folly::via(m_executor.get(),
        [this]() mutable { return m_client->getPipeline()->close(); })
        .get();

    m_executor->stop();
}

} // namespace communication
} // namespace one
