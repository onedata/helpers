/**
 * @file clientHandshakeRequest.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_MESSAGES_HANDSHAKE_REQUEST_H
#define HELPERS_MESSAGES_HANDSHAKE_REQUEST_H

#include "clientMessage.h"
#include "common.h"

#include <boost/optional.hpp>

#include <memory>
#include <string>
#include <vector>

namespace one {
namespace messages {

/**
 * The HandshakeRequest class represents a message that is sent by the client to
 * establish session.
 */
class ClientHandshakeRequest : public ClientMessage {
public:
    /**
     * Constructor.
     * @param sessionId Id of session to be used in handshake
     * */
    explicit ClientHandshakeRequest(std::string sessionId);

    /**
     * Constructor.
     * @param sessionId Id of session to be used in handshake
     * @param macaroon Access macaroon used to established session
     * @param version Client version
     */
    ClientHandshakeRequest(
        std::string sessionId, std::string macaroon, std::string version);

    /**
     * Constructor.
     * @param sessionId Id of session to be used in handshake
     * @param macaroon Access macaroon used to established session
     * @param version Client version
     * @param compatibleOneproviderVersions @deprecated
     * @param sessionMode Type of client session
     */
    ClientHandshakeRequest(std::string sessionId, std::string macaroon,
        std::string version,
        std::vector<std::string> compatibleOneproviderVersions,
        handshake::SessionMode sessionMode, handshake::ClientType clientType,
        std::vector<std::pair<std::string, std::string>> clientOptions,
        std::vector<std::pair<std::string, std::string>> systemInfo);

    std::string toString() const override;

private:
    std::unique_ptr<ProtocolClientMessage> serializeAndDestroy() override;

    std::string m_sessionId;
    boost::optional<std::string> m_macaroon;
    std::string m_version;
    std::vector<std::string> m_compatibleOneproviderVersions;
    handshake::SessionMode m_sessionMode;
    handshake::ClientType m_clientType;
    std::vector<std::pair<std::string, std::string>> m_clientOptions;
    std::vector<std::pair<std::string, std::string>> m_systemInfo;
};

} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_HANDSHAKE_REQUEST_H
