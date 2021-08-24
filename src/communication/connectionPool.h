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
}

/**
 * A @c ConnectionPool is responsible for managing connection pipeline
 * to the server.
 */
class ConnectionPool {
public:
    using Callback = std::function<void(const std::error_code &)>;

    /**
     * A reference to @c *this typed as a @c ConnectionPool.
     */
    ConnectionPool &connectionPool = *this;

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
     * @param providerTimeout Timeout for each request to a provider in seconds.
     */
    ConnectionPool(std::size_t connectionsNumber, std::size_t workersNumber,
        std::string host, uint16_t port, bool verifyServerCertificate,
        bool clprotoUpgrade = true, bool clprotoHandshake = true,
        const std::chrono::seconds providerTimeout = std::chrono::seconds{
            detail::kDefaultProviderTimeout});

    ConnectionPool(const ConnectionPool &) = delete;
    ConnectionPool &operator=(const ConnectionPool &) = delete;

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
    std::shared_ptr<folly::SSLContext> createSSLContext();

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
     * Destructor.
     * Calls @c stop().
     */
    virtual ~ConnectionPool();

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
    bool setupOpenSSLCABundlePath(SSL_CTX *ctx);

    const std::size_t m_connectionsNumber;
    const std::string m_host;
    const uint16_t m_port;
    const bool m_verifyServerCertificate;
    const std::chrono::seconds m_providerTimeout;
    const bool m_clprotoUpgrade;
    const bool m_clprotoHandshake;

    std::shared_ptr<const cert::CertificateData> m_certificateData;

    // Application level flag determining whether the connection pool is
    // connected or not. It does not mean that the connections are active as
    // they may have failed. This flag is set to true after first successfull
    // connection, and reset only after stop() is called.
    std::atomic<bool> m_connected;

    // Shared executor for the connection pool
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;

    // Pipeline factory for creating wangle handler pipelines for each
    // connection
    std::shared_ptr<CLProtoPipelineFactory> m_pipelineFactory;

    // Fixed pool of connection instances
    std::vector<std::shared_ptr<CLProtoClientBootstrap>> m_connections{};

    // Queue of pointers to currently idle connections from the fixed pool
    tbb::concurrent_bounded_queue<CLProtoClientBootstrap *> m_idleConnections{};

    std::function<void()> m_onConnectionLostCallback;
    std::function<void()> m_onReconnectCallback;
};

} // namespace communication
} // namespace one
