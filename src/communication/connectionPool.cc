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
#include <boost/filesystem.hpp>
#include <folly/Executor.h>
#include <folly/Range.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/IOBuf.h>
#include <iterator>
#include <tuple>
#include <utility>

namespace one {
namespace communication {

ConnectionPool::ConnectionPool(const std::size_t connectionsNumber,
    const std::size_t /*workersNumber*/, std::string host, const uint16_t port,
    const bool verifyServerCertificate, const bool clprotoUpgrade,
    const bool clprotoHandshake)
    : m_connectionsNumber{connectionsNumber}
    , m_host{std::move(host)}
    , m_port{port}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_clprotoUpgrade{clprotoUpgrade}
    , m_clprotoHandshake{clprotoHandshake}
    , m_connected{false}
    , m_executor{std::make_shared<folly::IOThreadPoolExecutor>(1)}
{
    LOG_FCALL() << LOG_FARG(connectionsNumber) << LOG_FARG(host)
                << LOG_FARG(port) << LOG_FARG(verifyServerCertificate);

    m_pipelineFactory =
        std::make_shared<CLProtoPipelineFactory>(m_clprotoUpgrade);

    for (unsigned int i = 0; i < m_connectionsNumber; i++) {
        auto client = std::make_shared<CLProtoClientBootstrap>(
            i, m_clprotoUpgrade, m_clprotoHandshake);
        client->group(m_executor);
        client->pipelineFactory(m_pipelineFactory);
        client->sslContext(createSSLContext());
        m_connections.emplace_back(std::move(client));
    }
}

bool ConnectionPool::setupOpenSSLCABundlePath(SSL_CTX *ctx)
{
    std::deque<std::string> caBundlePossibleLocations{
        "/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.trust.crt",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"};

    if (auto sslCertFileEnv = std::getenv("SSL_CERT_FILE")) {
        caBundlePossibleLocations.push_front(sslCertFileEnv);
    }

    auto it = std::find_if(caBundlePossibleLocations.begin(),
        caBundlePossibleLocations.end(), [](const auto &path) {
            namespace bf = boost::filesystem;
            return bf::exists(path) &&
                (bf::is_regular_file(path) || bf::is_symlink(path));
        });

    if (it != caBundlePossibleLocations.end()) {
        if (SSL_CTX_load_verify_locations(ctx, it->c_str(), nullptr) == 0) {
            LOG(ERROR)
                << "Invalid CA bundle at " << *it
                << ". Certificate server verification may not work properly...";
            return false;
        }

        return true;
    }

    return false;
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

    auto sslCtx = context->getSSLCtx();
    if (!setupOpenSSLCABundlePath(sslCtx)) {
        SSL_CTX_set_default_verify_paths(sslCtx);
    }

    // NOLINTNEXTLINE
    SSL_CTX_set_session_cache_mode(
        sslCtx, SSL_CTX_get_session_cache_mode(sslCtx) | SSL_SESS_CACHE_CLIENT);

    return context;
}

void ConnectionPool::connect()
{
    LOG_FCALL();

    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
    if (m_connected)
        return;

    for (auto &client : m_connections) {
        // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
        client->connect(m_host, m_port)
            .then(m_executor.get(), [ this, clientPtr = client.get() ]() {
                m_idleConnections.emplace(clientPtr);
            })
            .onError([this](folly::exception_wrapper ew) {
                close();
                ew.throw_exception();
            })
            .onError([this](std::exception &e) {
                close();
                throw e;
            })
            .get();
    }

    m_connected = true;

    LOG(INFO) << "All connections ready";
}

void ConnectionPool::setHandshake(
    const std::function<std::string()> &getHandshake,
    const std::function<std::error_code(std::string)> &onHandshakeResponse,
    const std::function<void(std::error_code)> &onHandshakeDone)
{
    LOG_FCALL();

    if (!m_clprotoHandshake)
        throw std::invalid_argument("CLProto handshake was explicitly disabled "
                                    "for this connection pool");

    m_pipelineFactory->setHandshake(
        getHandshake, onHandshakeResponse, onHandshakeDone);
}

void ConnectionPool::setOnMessageCallback(
    const std::function<void(std::string)> &onMessage)
{
    LOG_FCALL();

    m_pipelineFactory->setOnMessageCallback(onMessage);
}

void ConnectionPool::setCertificateData(
    const std::shared_ptr<cert::CertificateData> &certificateData)
{
    LOG_FCALL();

    m_certificateData = certificateData;
}

void ConnectionPool::send(
    const std::string &message, const Callback &callback, const int /*unused*/)
{
    LOG_FCALL() << LOG_FARG(message.size());

    if (!m_connected) {
        LOG_DBG(1) << "Connection pool stopped - cannot send message...";
        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    LOG_DBG(3) << "Attempting to send message of size " << message.size();

    CLProtoClientBootstrap *client = nullptr;
    try {
        LOG_DBG(3) << "Waiting for idle connection to become available";

        while ((client == nullptr) || !client->connected()) {
            if (!m_connected) {
                LOG_DBG(1)
                    << "Connection pool stopped - cannot send message...";
                folly::via(folly::getCPUExecutor().get(), [callback] {
                    callback(
                        std::make_error_code(std::errc::connection_aborted));
                });

                return;
            }

            m_idleConnections.pop(client);

            // If the client connection failed, try to reconnect
            // asynchronously
            if (!client->connected() && m_connected) {
                client->connect(m_host, m_port).then(m_executor.get(), [
                    this, client, executor = m_executor
                ]() {
                    if (m_connected)
                        // NOLINTNEXTLINE
                        m_idleConnections.emplace(client);
                    else
                        client->getPipeline()->close();
                });
                client = nullptr;
            }
        }

        LOG_DBG(3) << "Retrieved active connection " << client->connectionId()
                   << " from connection pool";
    }
    catch (const tbb::user_abort &) {
        // We have aborted the wait by calling stop()
        LOG_DBG(2) << "Waiting for connection from connection pool aborted";
        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    if (!client->getPipeline()->getTransport()->good()) {
        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    client->getPipeline()
        ->write(message)
        .then(folly::getCPUExecutor().get(),
            [callback] { callback(std::error_code{}); })
        .then(m_executor.get(), [this, client]() {
            m_idleConnections.emplace(client); // NOLINT
            LOG_DBG(3) << "Message sent";
        });

    LOG_DBG(3) << "Message queued";
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

    m_connected = false;

    close();

    m_executor->join();
}

void ConnectionPool::close()
{
    m_idleConnections.clear();

    // Cancel any threads waiting on m_idleConnections.pop()
    m_idleConnections.abort();

    for (const auto &client : m_connections) {
        if ((client->getPipeline() == nullptr) || !client->connected())
            continue;

        folly::via(m_executor.get(),
            [client]() mutable { return client->getPipeline()->close(); })
            .get();
    }
}

} // namespace communication
} // namespace one
