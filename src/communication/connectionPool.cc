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
    const std::size_t /*workersNumber*/, std::string host, const uint16_t port,
    const bool verifyServerCertificate, const bool clprotoUpgrade,
    const bool clprotoHandshake, const bool /* waitForReconnect */,
    const std::chrono::seconds providerTimeout)
    : m_connectionsNumber{connectionsNumber}
    , m_minConnectionsNumber{connectionsNumber == 0ULL
              ? 0ULL
              : std::min<std::size_t>(connectionsNumber, 2)}
    , m_host{std::move(host)}
    , m_port{port}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_providerTimeout{providerTimeout}
    , m_clprotoUpgrade{clprotoUpgrade}
    , m_clprotoHandshake{clprotoHandshake}
    , m_connectionState{State::CREATED}
    , m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
          1, std::make_shared<folly::NamedThreadFactory>("CPWork"))}
    , m_needMoreConnections{0}
    , m_sentMessageCounter{}
    , m_queuedMessageCounter{}
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

        if (areAllConnectionsDown()) {
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
    connectionMonitorTick();

    const auto connectStart = std::chrono::system_clock::now();

    // Wait for at least one connection to become live
    while (areAllConnectionsDown()) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10ms);

        if ((std::chrono::system_clock::now() - connectStart >
                m_providerTimeout) ||
            (m_connectionState == State::STOPPED)) {

            if (m_lastException) {
                rethrow_exception(m_lastException);
            }

            LOG(ERROR) << "Failed to establish connection to Oneprovider in "
                       << m_providerTimeout.count() << " [s]";

            throw std::system_error(std::make_error_code(std::errc::timed_out));
        }
    }

    auto connectDuration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - connectStart);

    LOG(INFO) << "Connection to Oneprovider " << m_host << " established in "
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
    folly::setThreadName("CPMon");

    using namespace std::chrono_literals;

    LOG_DBG(1) << "Starting connection monitor task";

    // This is just needed for mock tests
    if (m_connectionsNumber == 0)
        return;

    // Wait for the connection pool to start
    while (m_connectionState == State::CREATED) {
        std::this_thread::sleep_for(100ms);

        if (m_connectionState == State::STOPPED)
            return;
    }

    // Connection pool monitor task loop period for CONNECTED state
    const auto kConnectedMonitorSleepDuration{10s};

    // Connection pool monitor task loop period for CONNECTION_LOST state
    const auto kDisconnectedMonitorSleepDuration{5s};

    auto monitorSleepDuration{kConnectedMonitorSleepDuration};

    std::chrono::steady_clock::time_point lastIterationStart{};

    // Loop until connection pool is forcibly stopped using stop()
    // i.e. as long as m_connectionState != State::STOPPED
    while (true) {
        if (m_connectionState == State::STOPPED)
            break;

        // This condition variable is responsible for controlling loop
        // iteration - either the monitorSleepDuration expires or the
        // m_connectionMonitorWait variable is set to false, which breaks
        // out of continue loop, which is done by calling
        // connectionMonitorTick()
        LOG_DBG(3) << "Monitor task - waiting for tick signal for: "
                   << monitorSleepDuration.count() << " [s]";

        {
            std::unique_lock<std::mutex> lk(m_connectionMonitorMutex);
            m_connectionMonitorCV.wait_for(lk, monitorSleepDuration,
                [this] { return !m_connectionMonitorWait; });

            // Set monitor task loop to wait mode
            m_connectionMonitorWait = true;
        }

        LOG_DBG(3) << "Monitor task - starting monitoring chores";

        if (m_connectionState == State::STOPPED)
            break;

        lastIterationStart = std::chrono::steady_clock::now();

        // Make sure that the size of connection pool has at least
        // m_minConnectionsNumber elements
        ensureMinimumNumberOfConnections();

        // Add more connections if needed
        addNewConnectionOnDemand();

        // Loop over all connections and check their status, try to reconnect
        // if necessary
        {
            std::vector<std::pair<uint32_t, folly::Future<folly::Unit>>> futs;

            {
                std::lock_guard<std::mutex> guard{m_connectionsMutex};
                for (auto &client : m_connections) {
                    if (m_connectionState == State::STOPPED)
                        break;

                    LOG_DBG(3)
                        << "Checking connection " << client->connectionId()
                        << " status - " << (client->idle() ? "idle" : "taken");

                    // Skip working connections
                    if (client->connected())
                        continue;

                    LOG_DBG(3) << "Trying to renew connection "
                               << client->connectionId();

                    const int reconnectAttempt =
                        (m_connectionState == State::CONNECTION_LOST) ||
                            !client->firstConnection()
                        ? 1
                        : 0;

                    futs.emplace_back(client->connectionId(),
                        connectClient(client, reconnectAttempt));
                }
            }

            for (auto &f : futs) {
                try {
                    std::move(f.second)
                        .via(folly::getGlobalCPUExecutor().get())
                        .get();
                }
                catch (std::system_error &e) {
                    if (std::string{"handshake"} ==
                        e.code().category().name()) {
                        m_lastException = std::current_exception();
                        m_connectionState = State::STOPPED;
                        break;
                    }

                    throw e;
                }
                catch (...) {
                    LOG_DBG(1) << "Failed to reconnect connection " << f.first;

                    if (m_connectionState == State::CONNECTION_LOST) {
                        monitorSleepDuration =
                            std::min(3600s, monitorSleepDuration * 2);

                        LOG_DBG(1) << "Next reconnect retry in "
                                   << monitorSleepDuration.count() << " [s]";
                    }

                    continue;
                }

                LOG_DBG(2) << "Connection " << f.first << " is now live";

                if (m_connectionState == State::CONNECTION_LOST) {
                    if (m_onReconnectCallback)
                        m_onReconnectCallback();
                }

                if (m_connectionState == State::STOPPED)
                    break;

                m_connectionState = State::CONNECTED;

                monitorSleepDuration = kConnectedMonitorSleepDuration;
            }

            if (m_connectionState == State::STOPPED)
                break;

            if (m_connectionState == State::CONNECTION_LOST)
                monitorSleepDuration = kDisconnectedMonitorSleepDuration;

            {
                std::unique_lock<std::mutex> lk(m_connectionMonitorMutex);
                m_needMoreConnections = 0;
                m_connectionMonitorWait = true;
            }
        }
    }

    LOG_DBG(3) << "Exiting connection pool monitor task";
}

void ConnectionPool::addNewConnectionOnDemand()
{
    if (m_needMoreConnections > detail::kNeedMoreConnectionsThreshold &&
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
        .within(m_providerTimeout)
        .thenValue([this, clientPtr = client.get()](
                       auto && /*unit*/) { putClientBack(clientPtr); })
        .thenError(folly::tag_t<folly::exception_wrapper>{},
            [this, client](auto &&ew) {
                return folly::makeSemiFuture()
                    .via(folly::getGlobalCPUExecutor().get())
                    .thenValue(
                        [this, client](auto && /*unit*/) { return close(); })
                    .thenValue(
                        [ew](auto && /*unit*/) { ew.throw_exception(); });
            });
}

void ConnectionPool::putClientBack(CLProtoClientBootstrap *client)
{
    if (client != nullptr) {
        LOG_DBG(3) << "Putting back idle connection: " << client->connectionId()
                   << " [" << m_idleConnections.size() << "]";

        client->idle(true);

        m_idleConnections.emplace(client);
    }
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

void ConnectionPool::connectionMonitorTick()
{
    LOG_FCALL();

    std::lock_guard<std::mutex> lk{m_connectionMonitorMutex};
    m_connectionMonitorWait = false;
    m_connectionMonitorCV.notify_all();
}

folly::Future<ConnectionPool::IdleConnectionGuard>
ConnectionPool::getIdleClient(
    Callback callback, IdleConnectionGuard &&idleConnectionGuard)
{
    using namespace std::chrono_literals;

    CLProtoClientBootstrap *client{nullptr};

    while ((client == nullptr) || !client->connected()) {
        // First, check if the connection pool has been stopped
        // intentionally client-side
        if (m_connectionState == State::STOPPED) {
            LOG_DBG(1) << "Connection pool stopped - cannot send message...";

            folly::via(folly::getCPUExecutor().get(), [callback] {
                callback(std::make_error_code(std::errc::connection_aborted));
            });

            return std::move(idleConnectionGuard);
        }

        // Wait for an idle connection to become available
        if (!m_idleConnections.try_pop(client)) {

            if (m_connectionState == State::CONNECTED) {
                ++m_needMoreConnections;
                if (m_needMoreConnections >
                    detail::kNeedMoreConnectionsThreshold) {
                    connectionMonitorTick();
                }
            }

            const auto kMaximumIdleConnectionRetryWait{10s};
            const auto kIdleConnectionRetryBackoff{1.5};
            auto waitDelay = idleConnectionGuard.waitDelay();

            const auto soFar = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() -
                idleConnectionGuard.startedAt());
            const std::chrono::seconds writeTimeout = m_providerTimeout - soFar;

            if (writeTimeout.count() <= 0) {
                LOG_DBG(1) << "Idle connection not found before timeout - took "
                           << soFar.count();

                callback(std::make_error_code(std::errc::timed_out));
                return std::move(idleConnectionGuard);
            }

            if (waitDelay < kMaximumIdleConnectionRetryWait)
                idleConnectionGuard.setWaitDelay(
                    std::chrono::milliseconds{static_cast<size_t>(
                        waitDelay.count() * kIdleConnectionRetryBackoff)});

            LOG_DBG(1) << "No idle connection available (pool size "
                       << connectionsSize()
                       << ") - retry in: " << waitDelay.count() << " [ms]";

            return folly::makeSemiFuture()
                .via(m_executor.get())
                .delayed(waitDelay)
                .thenValue(
                    [this, callback = std::move(callback),
                        idleConnectionGuard = std::move(idleConnectionGuard)](
                        auto && /*unit*/) mutable {
                        return getIdleClient(
                            callback, std::move(idleConnectionGuard));
                    });
        }

        if (client != nullptr) {
            idleConnectionGuard.setClient(client);
            client->idle(false);

            LOG_DBG(1) << "Retrieved active connection "
                       << client->connectionId() << " from connection pool";

            break;
        }
    }

    return std::move(idleConnectionGuard);
}

void ConnectionPool::send(
    const std::string &message, const Callback &callback, const int /*unused*/)
{
    using namespace std::chrono_literals;

    LOG_FCALL() << LOG_FARG(message.size());

    if (m_connectionState == State::STOPPED || m_connectionsNumber == 0) {
        LOG_DBG(1) << "Connection pool stopped - cannot send message...";

        callback(std::make_error_code(std::errc::connection_aborted));
        return;
    }

    if (m_connectionState == State::CONNECTION_LOST)
        connectionMonitorTick();

    LOG_DBG(3) << "Attempting to send message of size " << message.size();

    folly::makeSemiFuture()
        .via(m_executor.get())
        .thenValue([this, callback](auto && /*unit*/) {
            return getIdleClient(callback, IdleConnectionGuard{this});
        })
        .thenValue([callback](
                       IdleConnectionGuard &&idleConnectionGuard) mutable {
            if (idleConnectionGuard.client() == nullptr)
                throw std::system_error{
                    std::make_error_code(std::errc::timed_out)};

            if (idleConnectionGuard.client()->getPipeline() == nullptr ||
                !idleConnectionGuard.client()
                     ->getPipeline()
                     ->getTransport()
                     ->good()) {
                callback(std::make_error_code(std::errc::connection_aborted));
                return std::move(idleConnectionGuard);
            }

            return std::move(idleConnectionGuard);
        })
        .thenValue([this, message, callback](
                       IdleConnectionGuard &&idleConnectionGuard) {
            const auto soFar = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() -
                idleConnectionGuard.startedAt());
            const std::chrono::seconds writeTimeout = m_providerTimeout - soFar;

            if (writeTimeout.count() <= 0) {
                LOG_DBG(1) << "Idle connection not found before timeout - took "
                           << soFar.count();

                throw std::system_error(
                    std::make_error_code(std::errc::timed_out));
            }

            auto *client = idleConnectionGuard.client();

            return client->getPipeline()
                ->write(message)
                .via(m_executor.get())
                .within(writeTimeout,
                    std::system_error{
                        std::make_error_code(std::errc::timed_out)})
                .thenTry([this, callback, connectionId = client->connectionId(),
                             idleConnectionGuard =
                                 std::move(idleConnectionGuard)](
                             folly::Try<folly::Unit> &&maybeUnit) {
                    if (maybeUnit.hasException()) {
                        LOG_DBG(1) << "Sending message failed due to: "
                                   << maybeUnit.exception().what();

                        if (areAllConnectionsDown()) {
                            if (m_onConnectionLostCallback)
                                m_onConnectionLostCallback();

                            m_connectionState = State::CONNECTION_LOST;
                        }

                        callback(std::make_error_code(std::errc::timed_out));
                    }
                    else {
                        LOG_DBG(3) << "Message sent: " << ++m_sentMessageCounter
                                   << " - queued: " << m_queuedMessageCounter
                                   << " [" << connectionId << "]";

                        callback(std::error_code{});
                    }

                    if (maybeUnit.hasException()) {
                        maybeUnit.throwIfFailed();
                    }
                });
        })
        .thenError(folly::tag_t<tbb::user_abort>{},
            [callback](
                auto && /*e*/) { // We have aborted the wait by calling stop()
                LOG(ERROR)
                    << "Waiting for connection from connection pool aborted";

                callback(std::make_error_code(std::errc::connection_aborted));
            })
        .thenError(folly::tag_t<std::system_error>{},
            [this, callback](auto &&e) {
                LOG(ERROR) << "Failed sending messages due to: " << e.what();

                if (e.code().value() == ETIMEDOUT) {
                    if (areAllConnectionsDown()) {
                        if (m_onConnectionLostCallback)
                            m_onConnectionLostCallback();

                        m_connectionState = State::CONNECTION_LOST;
                    }
                }

                callback(e.code());
            })
        .thenError(folly::tag_t<std::exception>{}, [callback](auto && /*e*/) {
            LOG(ERROR)
                << "Failed sending messages due to unknown connection error";

            callback(std::make_error_code(
                std::errc::resource_unavailable_try_again));
        });

    LOG_DBG(3) << "Message queued: " << ++m_queuedMessageCounter << " ["
               << "?"
               << "]";
}

ConnectionPool::~ConnectionPool()
{
    LOG_FCALL();

    stop();
}

void ConnectionPool::stop()
{
    LOG_FCALL();

    LOG_DBG(1) << "Stopping connection pool";

    if (m_connectionState == State::CREATED ||
        m_connectionState == State::STOPPED) {
        m_connectionState = State::STOPPED;

        connectionMonitorTick();

        if (m_connectionMonitorThread.joinable())
            m_connectionMonitorThread.join();
        return;
    }

    m_connectionState = State::STOPPED;

    connectionMonitorTick();

    if (m_connectionMonitorThread.joinable())
        m_connectionMonitorThread.join();

    close().get();

    m_executor->stop();

    LOG(INFO) << "Connection pool stopped";
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
