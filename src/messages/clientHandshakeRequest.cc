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
    , m_clientType{handshake::ClientType::oneclient}
{
}

ClientHandshakeRequest::ClientHandshakeRequest(
    std::string sessionId, std::string macaroon, std::string version)
    : m_sessionId{std::move(sessionId)}
    , m_macaroon{std::move(macaroon)}
    , m_version{std::move(version)}
    , m_sessionMode{handshake::SessionMode::normal}
    , m_clientType{handshake::ClientType::oneclient}
{
}

ClientHandshakeRequest::ClientHandshakeRequest(std::string sessionId,
    std::string macaroon, std::string version,
    std::vector<std::string> compatibleOneproviderVersions,
    handshake::SessionMode sessionMode, handshake::ClientType clientType,
    std::vector<std::pair<std::string, std::string>> clientOptions,
    std::vector<std::pair<std::string, std::string>> systemInfo)
    : m_sessionId{std::move(sessionId)}
    , m_macaroon{std::move(macaroon)}
    , m_version{std::move(version)}
    , m_compatibleOneproviderVersions{std::move(compatibleOneproviderVersions)}
    , m_sessionMode{sessionMode}
    , m_clientType{clientType}
    , m_clientOptions{std::move(clientOptions)}
    , m_systemInfo{std::move(systemInfo)}
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

    stream << ", client_type: ";
    if (m_clientType == handshake::ClientType::onedatafs)
        stream << "'ONEDATAFS'";
    else if (m_clientType == handshake::ClientType::ones3)
        stream << "'ONES3'";
    else
        stream << "'ONECLIENT'";

    return stream.str();
}

std::unique_ptr<ProtocolClientMessage>
ClientHandshakeRequest::serializeAndDestroy()
{
    auto clientMsg = std::make_unique<ProtocolClientMessage>();
    auto *handshakeRequestMsg = clientMsg->mutable_client_handshake_request();
    handshakeRequestMsg->set_session_id(m_sessionId);

    if (m_macaroon) {
        auto *macaroonMsg = handshakeRequestMsg->mutable_macaroon();
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

    if (m_clientType == handshake::ClientType::oneclient)
        handshakeRequestMsg->set_client_type(
            ::one::clproto::ClientType::ONECLIENT_TYPE);
    else if (m_clientType == handshake::ClientType::onedatafs)
        handshakeRequestMsg->set_client_type(
            ::one::clproto::ClientType::ONEDATAFS_TYPE);
    else
        handshakeRequestMsg->set_client_type(
            ::one::clproto::ClientType::ONES3_TYPE);

    for (const auto &kv : m_clientOptions) {
        auto *optionMsg = handshakeRequestMsg->add_client_options();
        optionMsg->set_name(kv.first);
        optionMsg->set_value(kv.second);
    }

    for (const auto &kv : m_systemInfo) {
        auto *infoMsg = handshakeRequestMsg->add_client_system_properties();
        infoMsg->set_name(kv.first);
        infoMsg->set_value(kv.second);
    }

    return clientMsg;
}

} // namespace messages
} // namespace one
