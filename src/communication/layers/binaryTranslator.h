/**
 * @file binaryTranslator.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_LAYERS_BINARY_TRANSLATOR_H
#define HELPERS_COMMUNICATION_LAYERS_BINARY_TRANSLATOR_H

#include "communication/declarations.h"
#include "helpers/logging.h"

#include <functional>
#include <memory>
#include <system_error>

namespace one {
namespace communication {
namespace layers {

/**
 * @c BinaryTranslator is responsible for translating @c ClientMessage protobuf
 * messages to string representation, and string representation back to
 * @c ServerMessage protobuf messages.
 */
template <class LowerLayer> class BinaryTranslator : public LowerLayer {
public:
    using Callback = typename LowerLayer::Callback;
    using LowerLayer::LowerLayer;
    using LowerLayer::send;

    virtual ~BinaryTranslator() = default; // NOLINT

    BinaryTranslator(const BinaryTranslator &) = delete;
    BinaryTranslator(BinaryTranslator &&) = delete;
    BinaryTranslator &operator=(const BinaryTranslator &) = delete;
    BinaryTranslator &operator=(BinaryTranslator &&) = delete;

    /**
     * A reference to @c *this typed as a @c BinaryTranslator.
     */
    BinaryTranslator<LowerLayer> &binaryTranslator = *this;

    /**
     * Wraps lower layer's @c setHandshake.
     * The handshake message is serialized into @c std::string and handshake
     * response is deserialized into a @c clproto::ServerMessage instance.
     * @see ConnectionPool::setHandshake()
     */
    auto setHandshake(std::function<ClientMessagePtr()> getHandshake,
        std::function<std::error_code(ServerMessagePtr)> onHandshakeResponse,
        std::function<void(std::error_code)> onHandshakeDone);

    /**
     * Wraps lower layer's @c setOnMessageCallback.
     * The incoming message is deserialized into a @c clproto::ServerMessage
     * instance.
     * @see ConnectionPool::setOnMessageCallback()
     */
    auto setOnMessageCallback(
        std::function<void(ServerMessagePtr)> onMessageCallback);

    /**
     * Serializes an instance of @c clproto::ClientMessage as @c std::string
     * and passes it down to the lower layer.
     * @param message The message to send.
     * @param retries The retries argument to pass to the lower layer.
     * @see ConnectionPool::send()
     */
    auto send(ClientMessagePtr message, Callback callback,
        int retries = DEFAULT_RETRY_NUMBER);
};

template <class LowerLayer>
auto BinaryTranslator<LowerLayer>::setHandshake(
    std::function<ClientMessagePtr()> getHandshake,
    std::function<std::error_code(ServerMessagePtr)> onHandshakeResponse,
    std::function<void(std::error_code)> onHandshakeDone)
{
    return LowerLayer::setHandshake(
        [getHandshake = std::move(getHandshake)] {
            return getHandshake()->SerializeAsString();
        },

        [onHandshakeResponse = std::move(onHandshakeResponse)](
            const std::string &message) {
            /// @todo A potential place for optimization [static serverMsg]
            auto serverMsg = std::make_unique<clproto::ServerMessage>();

            if (!serverMsg->ParseFromString(message)) {
                LOG(ERROR) << "Cannot parse protobuf message from binary "
                              "stream. Message size in bytes is "
                           << message.size();
                return std::make_error_code(std::errc::protocol_error);
            }

            return onHandshakeResponse(std::move(serverMsg));
        },

        std::move(onHandshakeDone));
}

template <class LowerLayer>
auto BinaryTranslator<LowerLayer>::setOnMessageCallback(
    std::function<void(ServerMessagePtr)> onMessageCallback)
{
    return LowerLayer::setOnMessageCallback([this,
                                                onMessageCallback = std::move(
                                                    onMessageCallback)](
                                                const std::string &message) {
        LOG_DBG(2) << "Received low level message of size: " << message.size();

        auto serverMsg = std::make_unique<clproto::ServerMessage>();
        if (serverMsg->ParseFromString(message)) {
            LOG_DBG(3) << "Received clproto message: "
                       << serverMsg->DebugString();

            LowerLayer::logServerMessage(*serverMsg);

            if (serverMsg->has_processing_status()) {
                LOG_DBG(2) << "Received ProcessingStatus heartbeat message - "
                              "ignoring...";
            }
            // Typically handshake errors are handled during handshake, but in
            // case user doesn't have any supported spaces in Oneprovider
            // this error can appear outside of a handshake exchange
            else if (serverMsg->has_handshake_response() &&
                serverMsg->handshake_response().status() ==
                    one::clproto::HandshakeStatus::INVALID_PROVIDER) {
                LOG_DBG(2) << "Received INVALID_PROVIDER handshake message";

                LowerLayer::setConnectionState(
                    LowerLayer::State::INVALID_PROVIDER);
            }
            else {
                onMessageCallback(std::move(serverMsg));
            }
        }
        else {
            LOG(ERROR) << "Received an invalid message from the server, not "
                          "compliant with clproto ServerMessage.";
        }
    });
}

template <class LowerLayer>
auto BinaryTranslator<LowerLayer>::send(
    ClientMessagePtr message, Callback callback, const int retries)
{
    LOG_DBG(3) << "Sending clproto message: " << message->DebugString();

    LowerLayer::logClientMessage(*message);

    /// @todo Possible optimization point here [static thread-local string]
    return LowerLayer::send(
        message->SerializeAsString(), std::move(callback), retries);
}

} // namespace layers
} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_LAYERS_BINARY_TRANSLATOR_H
