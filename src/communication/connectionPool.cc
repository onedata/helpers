/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "connectionPool.h"

#include "codec/clprotoHandshakeResponseHandler.h"
#include "codec/clprotoMessageHandler.h"
#include "codec/clprotoUpgradeResponseHandler.h"
#include "codec/packetDecoder.h"
#include "codec/packetEncoder.h"
#include "codec/packetLogger.h"
#include "exception.h"

#include <openssl/ssl.h>

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

#if defined(DEBUG_SSL_CONNECTION)
void log_ssl_info_callback(const SSL *s, int where, int ret)
{
    const char *str;
    int w;

    w = where & ~SSL_ST_MASK;

    if ((w & SSL_ST_CONNECT) != 0)
        str = "SSL_connect";
    else if ((w & SSL_ST_ACCEPT) != 0)
        str = "SSL_accept";
    else
        str = "undefined";

    if ((where & SSL_CB_LOOP) != 0) {
        LOG(ERROR) << str << ":" << SSL_state_string_long(s);
    }
    else if ((where & SSL_CB_ALERT) != 0) {
        str = (where & SSL_CB_READ) ? "read" : "write";
        LOG(ERROR) << "SSL3 alert " << str << ":"
                   << SSL_alert_type_string_long(ret) << ":"
                   << SSL_alert_desc_string_long(ret);
    }
    else if ((where & SSL_CB_EXIT) != 0) {
        if (ret == 0)
            LOG(ERROR) << str << ":failed in " << SSL_state_string_long(s);
        else if (ret < 0) {
            LOG(ERROR) << str << ":error in " << SSL_state_string_long(s);
        }
    }
}
#endif

ConnectionPool::ConnectionPool(const std::size_t connectionsNumber,
    const std::size_t workersNumber, std::string host, const uint16_t port,
    const bool verifyServerCertificate, const bool clprotoUpgrade,
    const bool clprotoHandshake, const std::chrono::seconds providerTimeout)
    : m_connectionsNumber{connectionsNumber}
    , m_host{std::move(host)}
    , m_port{port}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_providerTimeout{providerTimeout}
    , m_clprotoUpgrade{clprotoUpgrade}
    , m_clprotoHandshake{clprotoHandshake}
    , m_connected{false}
    , m_executor{std::make_shared<folly::IOThreadPoolExecutor>(workersNumber)}
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
        client->setEOFCallback([this]() {
            // Report to upper layers that all connections have been lost
            LOG_DBG(3) << "Entered EOF callback...";
            if (m_connected &&
                std::all_of(m_connections.begin(), m_connections.end(),
                    [](auto &c) { return !c->connected(); })) {
                LOG_DBG(2) << "Calling connection lost callback...";
                if (m_onConnectionLostCallback)
                    m_onConnectionLostCallback();
            }
        });
        m_connections.emplace_back(std::move(client));
    }
}

bool ConnectionPool::isConnected()
{
    return m_connected &&
        std::all_of(m_connections.begin(), m_connections.end(),
            [performCLProtoHandshake = m_clprotoHandshake](auto &c) {
                return c->connected() &&
                    (c->handshakeDone() || !performCLProtoHandshake);
            });
}

bool ConnectionPool::setupOpenSSLCABundlePath(SSL_CTX *ctx)
{
    std::deque<std::string> caBundlePossibleLocations{
        "/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.trust.crt",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"};

    if (auto *sslCertFileEnv = std::getenv("SSL_CERT_FILE")) {
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

std::shared_ptr<folly::SSLContext> ConnectionPool::createSSLContext() const
{
    auto context =
        std::make_shared<folly::SSLContext>(folly::SSLContext::TLSv1_2);

    context->authenticate(m_verifyServerCertificate, false);

    folly::ssl::setSignatureAlgorithms<folly::ssl::SSLCommonOptions>(*context);

    context->setVerificationOption(m_verifyServerCertificate
            ? folly::SSLContext::SSLVerifyPeerEnum::VERIFY
            : folly::SSLContext::SSLVerifyPeerEnum::NO_VERIFY);

    auto *sslCtx = context->getSSLCtx();
    if (!setupOpenSSLCABundlePath(sslCtx)) {
        SSL_CTX_set_default_verify_paths(sslCtx);
    }

    // NOLINTNEXTLINE
    SSL_CTX_set_session_cache_mode(
        sslCtx, SSL_CTX_get_session_cache_mode(sslCtx) | SSL_SESS_CACHE_CLIENT);

    // Limit the protocol to TLS1.2 as folly does not yet support 1.3
    SSL_CTX_set_max_proto_version(sslCtx, TLS1_2_VERSION);

#if defined(DEBUG_SSL_CONNECTION)
    SSL_CTX_set_info_callback(sslCtx, log_ssl_info_callback);
#endif

    return context;
}

void ConnectionPool::connect()
{
    LOG_FCALL();

    if (m_connected)
        return;

    for (auto &client : m_connections) {
        LOG(ERROR) << "CREATING CONNECTION TO " << m_host << ":" << m_port
                   << "\n";
        client->connect(m_host, m_port)
            .via(m_executor.get())
            .thenValue([this, clientPtr = client.get()](auto && /*unit*/) {
                LOG(ERROR) << "CREATED CONNECTION TO " << m_host << ":"
                           << m_port << "\n";
                m_idleConnections.emplace(clientPtr);
            })
            .thenError(folly::tag_t<folly::exception_wrapper>{},
                [this](auto &&ew) {
                    LOG(ERROR) << "FAILED TO CRETE CONNECTION TO " << m_host
                               << ":" << m_port << "\n";
                    return folly::makeSemiFuture()
                        .via(m_executor.get())
                        .thenValue([this](auto && /*unit*/) { return close(); })
                        .thenValue(
                            [ew](auto && /*unit*/) { ew.throw_exception(); });
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
        const auto sendStart = std::chrono::system_clock::now();
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

            if (!m_idleConnections.try_pop(client)) {
                LOG_DBG(3) << "Waiting for idle connection to become available";

                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1s);

                if (std::chrono::system_clock::now() - sendStart >
                    m_providerTimeout) {
                    folly::via(folly::getCPUExecutor().get(), [callback] {
                        callback(std::make_error_code(std::errc::timed_out));
                    });
                    return;
                }
                continue;
            }

            // If the client connection failed, try to reconnect
            // asynchronously
            if (!client->connected() && m_connected) {
                client->connect(m_host, m_port, 1)
                    .via(m_executor.get())
                    .thenValue([this, client, executor = m_executor](
                                   auto && /*unit*/) {
                        if (m_connected) {
                            // NOLINTNEXTLINE
                            m_idleConnections.emplace(client);
                            if (m_connected &&
                                std::all_of(m_connections.begin(),
                                    m_connections.end(),
                                    [](auto &c) { return c->connected(); }))
                                if (m_onReconnectCallback)
                                    m_onReconnectCallback();
                        }
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
    catch (const std::system_error &e) {
        LOG(ERROR) << "Failed sending messages due to: " << e.what();
        callback(e.code());
        return;
    }
    catch (...) {
        LOG(ERROR) << "Failed sending messages due to unknown connection error";
        callback(
            std::make_error_code(std::errc::resource_unavailable_try_again));
        return;
    }

    if (!client->getPipeline()->getTransport()->good()) {
        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    client->getPipeline()
        ->write(message)
        .via(folly::getCPUExecutor().get())
        .thenValue(
            [callback](auto && /*unit*/) { callback(std::error_code{}); })
        .via(m_executor.get())
        .thenValue([this, client](auto && /*unit*/) {
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

    close().get();

    m_executor->stop();
}

folly::Future<folly::Unit> ConnectionPool::close()
{
    m_idleConnections.clear();

    // Cancel any threads waiting on m_idleConnections.pop()
    m_idleConnections.abort();

    folly::fbvector<folly::Future<folly::Unit>> stopFutures;

    for (const auto &client : m_connections) {
        if ((client->getPipeline() == nullptr) || !client->connected())
            continue;

        stopFutures.emplace_back(
            folly::via(folly::getCPUExecutor().get())
                .thenValue([client](auto && /*unit*/) mutable {
                    return client->getPipeline()->close();
                }));
    }

    return folly::collectAll(stopFutures.begin(), stopFutures.end())
        .via(folly::getCPUExecutor().get())
        .thenValue([this](auto && /*res*/) {
            m_connections.clear();
            return folly::Future<folly::Unit>{};
        });
}

void ConnectionPool::setOnConnectionLostCallback(
    std::function<void()> onConnectionLostCallback)
{
    m_onConnectionLostCallback = std::move(onConnectionLostCallback);
}

void ConnectionPool::setOnReconnectCallback(
    std::function<void()> onReconnectCallback)
{
    m_onReconnectCallback = std::move(onReconnectCallback);
}

} // namespace communication
} // namespace one
