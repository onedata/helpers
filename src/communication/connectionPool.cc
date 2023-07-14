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
    const bool clprotoHandshake, const bool /* waitForReconnect */,
    const std::chrono::seconds providerTimeout)
    : m_connectionsNumber{connectionsNumber}
    , m_minConnectionsNumber{connectionsNumber < 1 ? 0ULL : 1}
    , m_host{std::move(host)}
    , m_port{port}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_providerTimeout{providerTimeout}
    , m_clprotoUpgrade{clprotoUpgrade}
    , m_clprotoHandshake{clprotoHandshake}
    , m_connectionState{State::CREATED}
    , m_executor{std::make_shared<folly::IOThreadPoolExecutor>(workersNumber)}
    , m_needMoreConnections{0}
{
    LOG_FCALL() << LOG_FARG(connectionsNumber) << LOG_FARG(host)
                << LOG_FARG(port) << LOG_FARG(verifyServerCertificate);

    m_pipelineFactory =
        std::make_shared<CLProtoPipelineFactory>(m_clprotoUpgrade);

    // Start connection monitor
    m_connectionMonitorThread =
        std::thread(&ConnectionPool::connectionMonitorTask, this);
}

void ConnectionPool::addConnection(int connectionId)
{
    LOG_DBG(1) << "Adding new connection " << connectionId
               << " to connection pool";

    auto client = std::make_shared<CLProtoClientBootstrap>(
        connectionId, m_clprotoUpgrade, m_clprotoHandshake);
    client->group(m_executor);
    client->pipelineFactory(m_pipelineFactory);
    client->sslContext(createSSLContext());
    client->setEOFCallback([this]() {
        // Report to upper layers that all connections have been lost
        LOG_DBG(3) << "Entered EOF callback...";

        if (m_connectionState == State::CONNECTED && areAllConnectionsDown()) {
            m_idleConnections.clear();

            std::lock_guard<std::mutex> guard{m_connectionsMutex};
            m_connections.clear();
        }
    });

    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    m_connections.emplace_back(std::move(client));
}

bool ConnectionPool::isConnected()
{
    return m_connectionState == State::CONNECTED && areAllConnectionsAlive();
}

bool ConnectionPool::areAllConnectionsAlive()
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};

    return std::all_of(m_connections.begin(), m_connections.end(),
        [performCLProtoHandshake = m_clprotoHandshake](auto &c) {
            return c->connected() &&
                (c->handshakeDone() || !performCLProtoHandshake);
        });
}

bool ConnectionPool::areAllConnectionsDown()
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};

    return std::all_of(m_connections.begin(), m_connections.end(),
        [](auto &c) { return !c->connected(); });
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

    if (m_connectionsNumber == 0)
        return;

    if (m_connectionState != State::CREATED)
        return;

    m_connectionState = State::CONNECTED;

    const auto connectStart = std::chrono::system_clock::now();

    // Wait for at least one connection to become live
    while (areAllConnectionsDown()) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10ms);

        if (std::chrono::system_clock::now() - connectStart >
            m_providerTimeout) {
            LOG(ERROR) << "Failed to establish connection to Oneprovider in "
                       << m_providerTimeout.count() << " [s]";
            throw std::system_error(std::make_error_code(std::errc::timed_out));
        }
    }

    auto connectDuration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - connectStart);

    LOG(INFO) << "Connection to Oneprovider established in "
              << connectDuration.count() << " [ms]";
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

void ConnectionPool::connectionMonitorTask()
{
    using namespace std::chrono_literals;

    if (m_connectionsNumber == 0)
        return;

    // Wait for the connection pool to start
    while (m_connectionState == State::CREATED) {
        std::this_thread::sleep_for(100ms);
    }

    while (m_connectionState != State::STOPPED) {
        // Make sure that the size of connection pool has at least
        // m_minConnectionsNumber elements
        ensureMinimumNumberOfConnections();

        // Add more connections if needed
        addNewConnectionOnDemand();

        {
            std::lock_guard<std::mutex> guard{m_connectionsMutex};
            for (auto &client : m_connections) {
                try {
                    // Skip working connections
                    if (client->connected())
                        continue;

                    connectClient(client, 0).get();
                }
                catch (...) {
                    LOG_DBG(1) << "Failed to reconnect connection "
                               << client->connectionId();
                    continue;
                }

                LOG_DBG(1) << "Connection " << client->connectionId()
                           << " is now live";

                if (m_connectionState == State::CONNECTION_LOST) {
                    if (m_onReconnectCallback)
                        m_onReconnectCallback();
                }

                m_connectionState = State::CONNECTED;
            }
        }

        if (m_connectionState == State::STOPPED)
            break;

        auto monitorSleepDuration = 5s;

        if (m_connectionState == State::CONNECTION_LOST)
            monitorSleepDuration = 30s;

        std::this_thread::sleep_for(monitorSleepDuration);
    }
}

void ConnectionPool::addNewConnectionOnDemand()
{
    const size_t kNeedMoreConnectionsThreshold = 4;
    if (m_needMoreConnections > kNeedMoreConnectionsThreshold &&
        connectionsSize() < m_connectionsNumber) {
        if (!areAllConnectionsDown())
            addConnection(connectionsSize());
        m_needMoreConnections = 0;
    }
}

folly::Future<folly::Unit> ConnectionPool::connectClient(
    std::shared_ptr<CLProtoClientBootstrap> client, int retries)
{
    return client->connect(m_host, m_port, retries)
        .via(m_executor.get())
        .thenValue([this, clientPtr = client.get()](auto && /*unit*/) {
            m_idleConnections.emplace(clientPtr);
        })
        .thenError(folly::tag_t<folly::exception_wrapper>{},
            [this, client](auto &&ew) {
                return folly::makeSemiFuture()
                    .via(m_executor.get())
                    .thenValue(
                        [this, client](auto && /*unit*/) { return close(); })
                    .thenValue(
                        [ew](auto && /*unit*/) { ew.throw_exception(); });
            });
}

void ConnectionPool::putClientBack(CLProtoClientBootstrap *client)
{
    if (client != nullptr)
        m_idleConnections.emplace(client);
}

size_t ConnectionPool::connectionsSize()
{
    std::lock_guard<std::mutex> guard{m_connectionsMutex};
    return m_connections.size();
}
void ConnectionPool::ensureMinimumNumberOfConnections()
{
    for (size_t i = connectionsSize(); i < m_minConnectionsNumber; i++) {
        addConnection(i);
    }
}

void ConnectionPool::send(
    const std::string &message, const Callback &callback, const int /*unused*/)
{
    LOG_FCALL() << LOG_FARG(message.size());

    if (m_connectionState == State::STOPPED || m_connectionsNumber == 0) {
        LOG_DBG(1) << "Connection pool stopped - cannot send message...";
        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    LOG_DBG(3) << "Attempting to send message of size " << message.size();

    CLProtoClientBootstrap *client = nullptr;
    const auto sendStart = std::chrono::system_clock::now();
    try {
        while ((client == nullptr) || !client->connected()) {
            // First, check if the connection pool has been stopped
            // intentionally client-side
            if (m_connectionState == State::STOPPED) {
                LOG_DBG(1)
                    << "Connection pool stopped - cannot send message...";
                folly::via(folly::getCPUExecutor().get(), [callback] {
                    callback(
                        std::make_error_code(std::errc::connection_aborted));
                });

                return;
            }

            // Wait for an idle connection to become available
            // If all connections are
            if (!m_idleConnections.try_pop(client)) {
                LOG_DBG(1) << "Waiting for an idle connection to become "
                              "available - current connection pool size "
                           << connectionsSize();

                if (!areAllConnectionsDown())
                    m_needMoreConnections.fetch_add(1);

                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1s);

                if (std::chrono::system_clock::now() - sendStart >
                    m_providerTimeout) {
                    // If sending message fails duration exceeds specified
                    // timeout - stop trying
                    throw std::system_error(
                        std::make_error_code(std::errc::timed_out));

                    return;
                }
                continue;
            }
        }

        LOG_DBG(3) << "Retrieved active connection " << client->connectionId()
                   << " from connection pool";
    }
    catch (const tbb::user_abort &) {
        // We have aborted the wait by calling stop()
        LOG_DBG(2) << "Waiting for connection from connection pool aborted";

        putClientBack(client);

        callback(std::make_error_code(std::errc::connection_aborted));

        return;
    }
    catch (const std::system_error &e) {
        LOG(ERROR) << "Failed sending messages due to: " << e.what();

        folly::via(folly::getCPUExecutor().get(),
            [callback, code = e.code()] { callback(code); });

        if (e.code().value() == ETIMEDOUT) {
            if (areAllConnectionsDown()) {
                if (m_onConnectionLostCallback)
                    m_onConnectionLostCallback();

                m_connectionState = State::CONNECTION_LOST;
            }
        }

        putClientBack(client);

        callback(e.code());
        return;
    }
    catch (...) {
        LOG(ERROR) << "Failed sending messages due to unknown connection error";

        putClientBack(client);

        callback(
            std::make_error_code(std::errc::resource_unavailable_try_again));
        return;
    }

    if (!client->getPipeline()->getTransport()->good()) {
        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    const auto now = std::chrono::system_clock::now();

    if (now - sendStart > m_providerTimeout) {
        LOG_DBG(1) << "Idle connection not found before timeout";

        folly::via(folly::getCPUExecutor().get(), [callback] {
            callback(std::make_error_code(std::errc::timed_out));
        });

        putClientBack(client);

        callback(std::make_error_code(std::errc::timed_out));
        return;
    }

    std::chrono::seconds sendRemainingTimeout = m_providerTimeout -
        std::chrono::duration_cast<std::chrono::seconds>(now - sendStart);

    client->getPipeline()
        ->write(message)
        .within(sendRemainingTimeout)
        .via(folly::getCPUExecutor().get())
        .thenValue(
            [callback](auto && /*unit*/) { callback(std::error_code{}); })
        .via(m_executor.get())

        .thenTry([this, client](folly::Try<folly::Unit> &&maybeUnit) {
            if (maybeUnit.hasException()) {
                LOG_DBG(1) << "Sending message failed due to: "
                           << maybeUnit.exception().what();
            }
            else
                LOG_DBG(3) << "Message sent";

            putClientBack(client);
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

    if (m_connectionState == State::CREATED ||
        m_connectionState == State::STOPPED) {
        if (m_connectionMonitorThread.joinable())
            m_connectionMonitorThread.join();
        return;
    }

    m_connectionState = State::STOPPED;

    close().get();

    if (m_connectionMonitorThread.joinable())
        m_connectionMonitorThread.join();

    m_executor->stop();
}

folly::Future<folly::Unit> ConnectionPool::close()
{
    m_idleConnections.clear();

    // Cancel any threads waiting on m_idleConnections.pop()
    m_idleConnections.abort();

    if (m_connections.empty())
        return folly::Future<folly::Unit>{};

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
            std::lock_guard<std::mutex> guard{m_connectionsMutex};
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
