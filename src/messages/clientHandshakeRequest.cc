/**
 * @file clientHandshakeRequest.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "messages/clientHandshakeRequest.h"

#include "messages.pb.h"

#include <sstream>

namespace one {
namespace messages {

ClientHandshakeRequest::ClientHandshakeRequest(std::string sessionId)
    : m_sessionId{std::move(sessionId)}
    , m_sessionMode{handshake::SessionMode::normal}
{
}

ClientHandshakeRequest::ClientHandshakeRequest(
    std::string sessionId, std::string macaroon, std::string version)
    : m_sessionId{std::move(sessionId)}
    , m_macaroon{std::move(macaroon)}
    , m_version{std::move(version)}
    , m_sessionMode{handshake::SessionMode::normal}
{
}

ClientHandshakeRequest::ClientHandshakeRequest(std::string sessionId,
    std::string macaroon, std::string version,
    std::vector<std::string> compatibleOneproviderVersions,
    handshake::SessionMode sessionMode)
    : m_sessionId{std::move(sessionId)}
    , m_macaroon{std::move(macaroon)}
    , m_version{std::move(version)}
    , m_compatibleOneproviderVersions{std::move(compatibleOneproviderVersions)}
    , m_sessionMode{sessionMode}
{
}

std::string ClientHandshakeRequest::toString() const
{
    std::stringstream stream;
    stream << "type: 'ClientHandshakeRequest', session ID: '" << m_sessionId
           << "', macaroon: ";

    if (m_macaroon)
        stream << "'" << m_macaroon.get() << "'";
    else
        stream << "'undefined'";

    stream << ", version: '" << m_version
           << "', compatible oneprovider versions:";
    for (const auto &compatibleVersion : m_compatibleOneproviderVersions)
        stream << " " << compatibleVersion;

    stream << ", session_mode: ";
    if (m_sessionMode == handshake::SessionMode::normal)
        stream << "'NORMAL'";
    else
        stream << "'OPEN_HANDLE'";

    return stream.str();
}

std::unique_ptr<ProtocolClientMessage>
ClientHandshakeRequest::serializeAndDestroy()
{
    auto clientMsg = std::make_unique<ProtocolClientMessage>();
    auto handshakeRequestMsg = clientMsg->mutable_client_handshake_request();
    handshakeRequestMsg->set_session_id(m_sessionId);

    if (m_macaroon) {
        auto macaroonMsg = handshakeRequestMsg->mutable_macaroon();
        macaroonMsg->set_macaroon(m_macaroon.get());
    }

    handshakeRequestMsg->set_version(m_version);
    for (const auto &compatibleVersion : m_compatibleOneproviderVersions)
        handshakeRequestMsg->add_compatible_oneprovider_versions(
            compatibleVersion);

    if (m_sessionMode == handshake::SessionMode::normal)
        handshakeRequestMsg->set_session_mode(
            ::one::clproto::SessionMode::NORMAL);
    else
        handshakeRequestMsg->set_session_mode(
            ::one::clproto::SessionMode::OPEN_HANDLE);

    return clientMsg;
}

} // namespace messages
} // namespace one
