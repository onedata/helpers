/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "clprotoPipelineFactory.h"
#include "logging.h"

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
     * that is then maintained by the @c ConnectionPool.
     */
    ConnectionPool(const std::size_t connectionsNumber,
        const std::size_t workersNumber, std::string host,
        const unsigned short port, const bool verifyServerCertificate,
        const bool clprotoUpgrade = true);

    /**
     * Creates connections to the remote endpoint specified in the constructor.
     * @note This method is separated from the constructor so that the
     * initialization can be augmented by other communication layers.
     */
    void connect();

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
    void setHandshake(std::function<std::string()> getHandshake,
        std::function<std::error_code(std::string)> onHandshakeResponse,
        std::function<void(std::error_code)> onHandshakeDone);

    /**
     * Sets a function to handle received messages.
     * @param onMessage The function handling received messages.
     */
    void setOnMessageCallback(std::function<void(std::string)> onMessage);

    /**
     * Sets certificate data to be used to authorize the client.
     * @param certificateData The certificate data to set.
     */
    void setCertificateData(
        std::shared_ptr<cert::CertificateData> certificateData);

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
    void send(std::string message, Callback callback, const int = int{});

    /**
     * Destructor.
     * Calls @c stop().
     */
    virtual ~ConnectionPool();

private:
    /**
     * Stops the @c ConnectionPool operations.
     * All connections are dropped. This method exists to break the wait of any
     * threads waiting in @c send. It is designed to be called by the destructor
     * or after a failure to connect internally by the connection pool.
     */
    void stop();

    const std::size_t m_connectionsNumber;
    const std::size_t m_workersNumber;
    std::string m_host;
    const unsigned short m_port;
    folly::SocketAddress m_address;
    const bool m_verifyServerCertificate;
    const bool m_clprotoUpgrade;
    std::shared_ptr<const cert::CertificateData> m_certificateData;

    std::atomic<bool> m_connected;

    std::function<std::string()> m_getHandshake;
    std::function<std::error_code(std::string)> m_onHandshakeResponse;
    std::function<void(std::error_code)> m_onHandshakeDone;

    std::function<void(std::string)> m_onMessage = [](auto) {};
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    std::shared_ptr<CLProtoClientBootstrap> m_client;
    std::shared_ptr<CLProtoPipelineFactory> m_pipelineFactory;

    std::string makeUpgradeRequest();
    std::string makeHandshake();
};

} // namespace communication
} // namespace one
