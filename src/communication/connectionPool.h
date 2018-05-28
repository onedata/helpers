/**
 * @file connectionPool.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_CONNECTION_POOL_H
#define HELPERS_COMMUNICATION_CONNECTION_POOL_H

#include "connection.h"

#include <asio/ssl/context.hpp>
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
}

/**
 * A @c ConnectionPool is responsible for managing instances of @c Connection.
 * It provides a facade for the connections, ensuring that outside entities
 * do not interact with connections directly.
 */
class ConnectionPool {
public:
    using Callback = Connection::Callback;
    using ConnectionFactory = std::function<std::shared_ptr<Connection>(
        std::string, const unsigned short, asio::io_service &,
        std::shared_ptr<asio::ssl::context>, std::function<void(std::string)>,
        std::function<void(Connection &)>, std::function<std::string()>,
        std::function<std::error_code(std::string)>,
        std::function<void(std::error_code)>)>;

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
     * @param connectionFactory A function that returns a new connection object
     * that is then maintained by the @c ConnectionPool.
     */
    ConnectionPool(const std::size_t connectionsNumber,
        const std::size_t workersNumber, std::string host,
        const unsigned short port, const bool verifyServerCertificate,
        ConnectionFactory connectionFactory);

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
     * Sends a message through one of the managed connections.
     * Returns immediately if @c connect() has not been called, or @c stop() has
     * been called.
     * @param message The message to send.
     * @param callback Callback function that is called on send success or
     * error.
     */
    void send(std::string message, Callback callback, const int = int{});

    std::string makeHttpRequest(const std::string &token,
        const std::string &type, const std::string &endpoint,
        const std::string &contentType, const std::string &body);

    /**
     * Destructor.
     * Calls @c stop().
     */
    virtual ~ConnectionPool();

    /**
     * Stops the @c ConnectionPool operations.
     * All connections are dropped. This method exists to break the wait of any
     * threads waiting in @c send. It is designed to be called at the end of the
     * main application thread.
     */
    void stop();

    /**
     * Return the reference to the underlying io_service instance.
     */
    asio::io_service &ioService();

private:
    void onConnectionReady(Connection &conn);

    std::atomic<bool> m_connected{false};
    const std::size_t m_connectionsNumber;
    const std::size_t m_workersNumber;
    std::string m_host;
    const unsigned short m_port;
    const bool m_verifyServerCertificate;
    std::shared_ptr<const cert::CertificateData> m_certificateData;

    std::function<std::string()> m_getHandshake;
    std::function<std::error_code(std::string)> m_onHandshakeResponse;
    std::function<void(std::error_code)> m_onHandshakeDone;
    ConnectionFactory m_connectionFactory;

    std::function<void(std::string)> m_onMessage = [](auto) {};

    // Main io_service instance for the connection pool
    asio::io_service m_ioService;
    asio::executor_work_guard<asio::io_service::executor_type> m_work{
        asio::make_work_guard(m_ioService)};
    // Pool of worker threads to handle connection pool
    std::vector<std::thread> m_poolWorkers;

    std::shared_ptr<asio::ssl::context> m_context{
        std::make_shared<asio::ssl::context>(
            asio::ssl::context::tlsv12_client)};

    // Fixed pool of connection instances
    std::vector<std::shared_ptr<Connection>> m_connections;
    // Queue of pointers to currently idle connections from the fixed pool
    tbb::concurrent_bounded_queue<Connection *> m_idleConnections;
};

} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_CONNECTION_POOL_H
