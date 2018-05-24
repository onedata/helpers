/**
 * @file connectionPool.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "connectionPool.h"

#include "cert/certificateData.h"
#include "etls/utils.h"
#include "exception.h"
#include "logging.h"

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <tuple>

using namespace std::placeholders;
using namespace std::literals::chrono_literals;

namespace one {
namespace communication {

ConnectionPool::ConnectionPool(const std::size_t connectionsNumber,
    const std::size_t workersNumber, std::string host,
    const unsigned short port, const bool verifyServerCertificate,
    ConnectionFactory connectionFactory)
    : m_connectionsNumber{connectionsNumber}
    , m_workersNumber{workersNumber}
    , m_host{std::move(host)}
    , m_port{port}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_connectionFactory{std::move(connectionFactory)}
{
    LOG_FCALL() << LOG_FARG(connectionsNumber) << LOG_FARG(host)
                << LOG_FARG(port) << LOG_FARG(verifyServerCertificate);

    for (std::size_t i = 0; i < m_workersNumber; i++) {
        m_poolWorkers.emplace_back([&, tid = i ] {
            etls::utils::nameThread("CPWorker-" + std::to_string(tid));
            m_ioService.run();
        });
    }
}

void ConnectionPool::connect()
{
    LOG_FCALL();

    m_context->set_options(asio::ssl::context::default_workarounds |
        asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3 |
        asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1 |
        asio::ssl::context::single_dh_use);

    m_context->set_default_verify_paths();
    m_context->set_verify_mode(m_verifyServerCertificate
            ? asio::ssl::verify_peer
            : asio::ssl::verify_none);

    SSL_CTX *ssl_ctx = m_context->native_handle();
    auto mode = SSL_CTX_get_session_cache_mode(ssl_ctx) | SSL_SESS_CACHE_CLIENT;

    SSL_CTX_set_session_cache_mode(ssl_ctx, mode);

    if (m_certificateData)
        m_certificateData->initContext(*m_context);

    std::generate_n(
        std::back_inserter(m_connections), m_connectionsNumber, [&] {
            auto connection = m_connectionFactory(m_host, m_port, m_ioService,
                m_context, asio::bind_executor(m_ioService, m_onMessage),
                std::bind(&ConnectionPool::onConnectionReady, this, _1),
                m_getHandshake, m_onHandshakeResponse, m_onHandshakeDone);

            LOG_DBG(1) << "Creating connection in connection pool to "
                       << m_host;

            connection->connect();

            return connection;
        });

    m_connected = true;
}

std::string ConnectionPool::makeHttpRequest(const std::string &token,
    const std::string &type, const std::string &endpoint,
    const std::string &contentType, const std::string &body)
{
    asio::io_service io_service;

    using asio::ip::tcp;

    // Get a list of endpoints corresponding to the server name.
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(m_host, "https");
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

    // Try each endpoint until we successfully establish a connection.
    asio::ssl::stream<tcp::socket> socket(io_service, *m_context);
    asio::connect(socket.lowest_layer(), endpoint_iterator);
    socket.lowest_layer().set_option(tcp::no_delay(true));
    socket.handshake(asio::ssl::stream_base::client);

    // Form the request. We specify the "Connection: close" header so that the
    // server will close the socket after transmitting the response. This will
    // allow us to treat all data up until the EOF as the content.
    asio::streambuf request;
    std::ostream requestStream(&request);
    requestStream << type << " " << endpoint << " HTTP/1.1\r\n";
    requestStream << "Host: " << m_host << "\r\n";
    requestStream << "User-agent: Oneclient\r\n";
    requestStream << "Accept: */*\r\n";
    requestStream << "X-Auth-Token: " << token << "\r\n";
    if (contentType.size() > 0)
        requestStream << "Content-type: " << contentType << "\r\n";
    requestStream << "Content-length: " << std::to_string(body.size())
                  << "\r\n";
    requestStream << "\r\n";
    if (body.size() > 0)
        requestStream << body;
    requestStream.flush();

    // Send the request.
    asio::write(socket, request);

    // Read the response status line. The response streambuf will automatically
    // grow to accommodate the entire line. The growth may be limited by passing
    // a maximum size to the streambuf constructor.
    asio::streambuf response;
    asio::read_until(socket, response, "\r\n");

    // Check that response is OK.
    std::istream response_stream(&response);
    std::string http_version;
    response_stream >> http_version;
    unsigned int status_code;
    response_stream >> status_code;
    std::string status_message;
    std::getline(response_stream, status_message);
    if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
        LOG(ERROR) << "Invalid HTTP REST response";
        throw std::runtime_error("Invalid HTTP REST response");
    }
    if (!(status_code == 200 || status_code == 204)) {
        LOG(ERROR) << "HTTP REST response returned with status code: "
                   << status_code << "\n";
        throw std::runtime_error(
            "HTTP REST error: " + std::to_string(status_code));
    }

    // Read the response headers, which are terminated by a blank line.
    asio::read_until(socket, response, "\r\n\r\n");

    // Process the response headers.
    std::string header;
    while (std::getline(response_stream, header) && header != "\r")
        LOG(INFO) << "Received HTTP REST response header: " << header << "\n";

    std::stringstream responseContent;

    // Write whatever content we already have to output.
    if (response.size() > 0)
        responseContent << &response;

    // Read until EOF, writing data to output as we go.
    asio::error_code error;
    while (asio::read(socket, response, asio::transfer_at_least(1), error))
        responseContent << &response;

    if (error != asio::error::eof)
        throw std::runtime_error(error.category().name());

    return responseContent.str();
}

asio::io_service &ConnectionPool::ioService() { return m_ioService; }

void ConnectionPool::setHandshake(std::function<std::string()> getHandshake,
    std::function<std::error_code(std::string)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
{
    LOG_FCALL();

    m_getHandshake = std::move(getHandshake);
    m_onHandshakeResponse = std::move(onHandshakeResponse);
    m_onHandshakeDone = std::move(onHandshakeDone);
}

void ConnectionPool::setOnMessageCallback(
    std::function<void(std::string)> onMessage)
{
    LOG_FCALL();

    m_onMessage = std::move(onMessage);
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

    LOG_DBG(2) << "Attempting to send message of size " << message.size();

    if (!m_connected) {
        return;
    }

    Connection *conn = nullptr;
    try {
        LOG_DBG(2) << "Waiting for idle connection to become available";
        while (!conn || !conn->connected()) {
            m_idleConnections.pop(conn);
        }
        LOG_DBG(2) << "Retrieved active connection from connection pool";
    }
    catch (const tbb::user_abort &) {
        // We have aborted the wait by calling stop()
        LOG(ERROR) << "Waiting for connection from connection pool aborted";
        return;
    }

    // There might be a case that the connection has failed between
    // inserting it into ready queue and popping it here; that's ok
    // since connection will fail the send instead of erroring out.
    conn->send(std::move(message), std::move(callback));

    LOG_DBG(2) << "Message sent";
}

void ConnectionPool::onConnectionReady(Connection &conn)
{
    LOG_FCALL();

    LOG_DBG(2) << "Connection ready - adding to idle connection pool";

    m_idleConnections.emplace(&conn);
}

ConnectionPool::~ConnectionPool()
{
    LOG_FCALL();

    stop();
}

void ConnectionPool::stop()
{
    LOG_FCALL();

    LOG_DBG(1) << "Stopping connection pool...";

    m_connected = false;
    m_connections.clear();
    m_idleConnections.abort();

    if (!m_ioService.stopped())
        m_ioService.stop();

    for (auto &worker : m_poolWorkers) {
        if (worker.joinable())
            worker.join();
    }

    LOG_DBG(1) << "Connection pool stopped";
}

} // namespace communication
} // namespace one
