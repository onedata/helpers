/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "clprotoClientBootstrap.h"
#include "clprotoPipelineFactory.h"
#include "helpers/logging.h"

#include <tbb/concurrent_queue.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace one {
namespace communication {

namespace cert {
class CertificateData;
} // namespace cert

namespace detail {
const auto kDefaultProviderTimeout = 120UL;
} // namespace detail

/**
 * A @c ConnectionPool is responsible for managing connection pipeline
 * to the server.
 */
class ConnectionPool {
public:
    using Callback = std::function<void(const std::error_code &)>;

    enum class State {
        CREATED,   /*< Connection pool has been created, but not started yet */
        CONNECTED, /*< Connection pool is connected or connecting */
        CONNECTION_LOST, /*< Connection has been lost for a time longer than
                            timeout period */
        STOPPED /*< Connection pool has been stopped, clean up resources */
    };

    /**
     * IdleConnectionGuard ensures that each connection taken from connection
     * pool, is returned after a message is sent.
     */
    class IdleConnectionGuard {
    public:
        explicit IdleConnectionGuard(ConnectionPool *pool)
            : m_pool{pool}
        {
            assert(m_pool != nullptr);
        }

        IdleConnectionGuard(IdleConnectionGuard &&other) noexcept
        {
            if (this == &other)
                return;

            m_pool = other.m_pool;
            m_client = other.m_client;
            other.m_pool = nullptr;
            other.m_client = nullptr;
        }

        IdleConnectionGuard &operator=(IdleConnectionGuard &&other) noexcept
        {
            if (this == &other)
                return *this;

            m_pool = other.m_pool;
            m_client = other.m_client;
            other.m_pool = nullptr;
            other.m_client = nullptr;

            return *this;
        }

        IdleConnectionGuard(const IdleConnectionGuard &) = delete;
        IdleConnectionGuard &operator=(IdleConnectionGuard const &) = delete;

        ~IdleConnectionGuard()
        {
            if (m_pool != nullptr)
                m_pool->putClientBack(m_client);
        }

        void setClient(CLProtoClientBootstrap *client) { m_client = client; }

    private:
        ConnectionPool *m_pool{nullptr};
        CLProtoClientBootstrap *m_client{nullptr};
    };

    /**
     * Constructor.
     * @param connectionsNumber Number of connections that should be maintained
     * by this pool.
     * @param workersNumber Number of worker threads that should be maintained
     * by this pool.
     * @param host Hostname of the remote endpoint.
     * @param port Port number of the remote endpoint.
     * @param verifyServerCertificate Specifies whether to verify server's
     * SSL certificate.
     * @param clprotoUpgrade Flag determining whether connections should request
     * upgrade to clproto protocol after connection.
     * @param clprotoHandshake Flag determining whether connections should
     * perform clproto handshake after upgrading to clproto.
     * @param waitForReconnect If true, wait for connections to resume until
     *                         providerTimeout, if false, return ECONNRESET
     * @param providerTimeout Timeout for each request to a provider in seconds.
     */
    ConnectionPool(std::size_t connectionsNumber, std::size_t workersNumber,
        std::string host, uint16_t port, bool verifyServerCertificate,
        bool clprotoUpgrade = true, bool clprotoHandshake = true,
        bool waitForReconnect = false,
        std::chrono::seconds providerTimeout = std::chrono::seconds{
            detail::kDefaultProviderTimeout});

    ConnectionPool(const ConnectionPool &) = delete;
    ConnectionPool &operator=(const ConnectionPool &) = delete;
    ConnectionPool(ConnectionPool &&) = delete;
    ConnectionPool &operator=(ConnectionPool &&) = delete;

    /**
     * Destructor.
     * Calls @c stop().
     */
    virtual ~ConnectionPool();

    /**
     * Creates connections to the remote endpoint specified in the constructor.
     * @note This method is separated from the constructor so that the
     * initialization can be augmented by other communication layers.
     */
    void connect();

    /**
     * Checks if the connection to Oneprovider is functional
     * and that handshake has been successfully completed.
     */
    bool isConnected();

    /**
     * Sets handshake-related functions.
     * The handshake functions are passed down to connections and used on
     * initialization of each TCP connection.
     * @param getHandshake A function that returns a handshake to send through
     * connections.
     * @param onHandshakeResponse A function that takes a handshake response.
     * @param onHandshakeDone A function that is called whenever handshake
     * succeeds or fails.
     * @note This method is separated from constructor so that the handshake
     * messages can be translated by other communication layers.
     */
    void setHandshake(const std::function<std::string()> &getHandshake,
        const std::function<std::error_code(std::string)> &onHandshakeResponse,
        const std::function<void(std::error_code)> &onHandshakeDone);

    /**
     * Sets a function to handle received messages.
     * @param onMessage The function handling received messages.
     */
    void setOnMessageCallback(
        const std::function<void(std::string)> &onMessage);

    /**
     * Sets certificate data to be used to authorize the client.
     * @param certificateData The certificate data to set.
     */
    void setCertificateData(
        const std::shared_ptr<cert::CertificateData> &certificateData);

    /**
     * Initialize the SSL context for communication sockets.
     */
    std::shared_ptr<folly::SSLContext> createSSLContext() const;

    /**
     * Sends a message through one of the managed connections.
     * Returns immediately if @c connect() has not been called, or @c stop() has
     * been called.
     * @param message The message to send.
     * @param callback Callback function that is called on send success or
     * error.
     */
    void send(const std::string &message, const Callback &callback,
        int /*unused*/ = int{});

    /**
     * Stops the @c ConnectionPool operations.
     * All connections are dropped. This method exists to break the wait of any
     * threads waiting in @c send.
     */
    void stop();

    std::shared_ptr<folly::Executor> executor() { return m_executor; }

    void setOnConnectionLostCallback(
        std::function<void()> onConnectionLostCallback);

    void setOnReconnectCallback(std::function<void()> onReconnectCallback);

private:
    void connectionMonitorTick();

    void addConnection(int connectionId);

    void connectionMonitorTask();

    bool areAllConnectionsAlive();

    bool areAllConnectionsDown();

    void ensureMinimumNumberOfConnections();

    void addNewConnectionOnDemand();

    folly::Future<folly::Unit> connectClient(
        std::shared_ptr<CLProtoClientBootstrap> client, int retries);

    void putClientBack(CLProtoClientBootstrap *client);

    size_t connectionsSize();

    /**
     * Close connections and handler pipelines.
     */
    folly::Future<folly::Unit> close();

    /**
     * Setup trusted CA certificates by trying to find a CA file in on of common
     * locations, otherwise ask OpenSSL to set up default paths.
     * @param ctx OpenSSL context
     * @return True when CA file was found and loaded successfuly, false
     * otherwise
     */
    static bool setupOpenSSLCABundlePath(SSL_CTX *ctx);

    /** Maximum connections number */
    const std::size_t m_connectionsNumber;
    /** Minimum connections number */
    const std::size_t m_minConnectionsNumber;
    const std::string m_host;
    const uint16_t m_port;
    const bool m_verifyServerCertificate;
    const std::chrono::seconds m_providerTimeout;
    const bool m_clprotoUpgrade;
    const bool m_clprotoHandshake;

    std::shared_ptr<const cert::CertificateData> m_certificateData;

    // Application level state determining whether the connection pool is
    // connected or not.
    std::atomic<State> m_connectionState;

    // Shared executor for the connection pool
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;

    // Pipeline factory for creating wangle handler pipelines for each
    // connection
    std::shared_ptr<CLProtoPipelineFactory> m_pipelineFactory;

    std::mutex m_connectionMonitorMutex;
    std::condition_variable m_connectionMonitorCV;
    bool m_connectionMonitorWait{true};

    // Fixed pool of connection instances
    std::mutex m_connectionsMutex;
    std::vector<std::shared_ptr<CLProtoClientBootstrap>> m_connections{};

    // Queue of pointers to currently idle connections from the fixed pool
    tbb::concurrent_bounded_queue<CLProtoClientBootstrap *> m_idleConnections{};

    std::function<void()> m_onConnectionLostCallback;
    std::function<void()> m_onReconnectCallback;

    std::thread m_connectionMonitorThread;
    std::atomic<size_t> m_needMoreConnections;
};

} // namespace communication
} // namespace one
