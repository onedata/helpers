/**
 * @file translator.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_LAYERS_TRANSLATOR_H
#define HELPERS_COMMUNICATION_LAYERS_TRANSLATOR_H

#include "communication/declarations.h"
#include "fuseOperations.h"
#include "helpers/logging.h"
#include "messages/clientHandshakeRequest.h"
#include "messages/clientMessage.h"
#include "messages/handshakeResponse.h"
#include "messages/serverMessage.h"

#include <folly/futures/Future.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <future>
#include <mutex>
#include <system_error>

namespace one {
namespace communication {

namespace layers {

/**
 * @c Translator is responsible for translating between domain objects and
 * @c clproto objects.
 */
template <class LowerLayer> class Translator : public LowerLayer {
public:
    using LowerLayer::LowerLayer;
    using LowerLayer::send;

    template <typename SrvMsg>
    using CommunicateCallback =
        std::function<void(const std::error_code &ec, std::unique_ptr<SrvMsg>)>;

    virtual ~Translator() = default; // NOLINT

    Translator(const Translator &) = delete;
    Translator(Translator &&) = delete;
    Translator &operator=(const Translator &) = delete;
    Translator &operator=(Translator &&) = delete;

    /**
     * A reference to @c *this typed as a @c Translator.
     */
    Translator<LowerLayer> &translator = *this;

    /**
     * Serializes an instance of @c message::client::ClientMessage as
     * @c clproto::ClientMessage and passes it down to the lower layer.
     * @param message The message to send.
     * @param retires The retries argument to pass to the lower layer.
     * @see ConnectionPool::send()
     */
    folly::Future<folly::Unit> send(
        messages::ClientMessage &&msg, int retries = DEFAULT_RETRY_NUMBER);

    /**
     * Wraps lower layer's @c setHandshake.
     * The handshake message is serialized into @c one::clproto::ClientMessage
     * and handshake response is deserialized into a
     * @c one::clproto::ServerMessage instance.
     * @see ConnectionPool::setHandshake()
     * @note This method is only instantiable if the lower layer has a
     * @c setHandshake method.
     * @return A future containing the status of the first handshake.
     */
    template <typename = void>
    auto setHandshake(
        std::function<one::messages::ClientHandshakeRequest()> getHandshake,
        std::function<std::error_code(one::messages::HandshakeResponse)>
            onHandshakeResponse)
    {
        auto promise = std::make_shared<folly::Promise<folly::Unit>>();
        auto callOnceFlag = std::make_shared<std::once_flag>();

        LowerLayer::setHandshake(
            [getHandshake = std::move(getHandshake)] {
                return messages::serialize(getHandshake());
            },
            [onHandshakeResponse = std::move(onHandshakeResponse)](
                ServerMessagePtr msg) {
                return onHandshakeResponse({std::move(msg)});
            },
            [promise, callOnceFlag](const std::error_code &ec) {
                std::call_once(*callOnceFlag, [&] {
                    if (ec) {
                        LOG(ERROR) << "Handshake error: " << ec.message() << "("
                                   << ec.value() << ")";
                        promise->setException(std::system_error{ec});
                    }
                    else
                        promise->setValue();
                });
            });

        return promise->getFuture();
    }

    /**
     * Wraps lower layer's @c reply.
     * The outgoing message is serialized as in @c send().
     * @see Replier::reply()
     * @note This method is only instantiable if the lower layer has a @c reply
     * method.
     */
    template <typename = void>
    auto reply(const clproto::ServerMessage &replyTo,
        messages::ClientMessage &&msg, const int retry = DEFAULT_RETRY_NUMBER)
    {
        LOG_FCALL() << LOG_FARG(retry);

        LOG_DBG(3) << "Replying to server message id: " << replyTo.message_id()
                   << " with message: {" << msg.toString() << "}";

        auto promise = std::make_shared<folly::Promise<folly::Unit>>();

        auto callback = [promise](const std::error_code &ec) {
            if (ec) {
                LOG(ERROR) << "Reply error: " << ec.message() << "("
                           << ec.value() << ")";
                promise->setException(std::system_error{ec});
            }
            else
                promise->setValue();
        };

        LowerLayer::reply(replyTo, messages::serialize(std::move(msg)),
            std::move(callback), retry);

        return promise->getFuture();
    }

    /**
     * Wraps lower layer's @c communicate.
     * The ougoing message is serialized as in @c send().
     * @see Inbox::communicate()
     * @note This method is only instantiable if the lower layer has a
     * @c communicate method.
     * @return A future representing peer's answer.
     */
    template <class SvrMsg, class CliMsg>
    folly::Future<SvrMsg> communicate(
        CliMsg &&msg, const int retries = DEFAULT_RETRY_NUMBER)
    {
        LOG_FCALL() << LOG_FARG(retries);

        LOG_DBG(4) << "Communicating clproto message: {" << msg.toString()
                   << "}";

        auto promise = std::make_shared<folly::Promise<SvrMsg>>();
        auto future =
            promise->getSemiFuture().via(LowerLayer::executor().get());
        auto callback = [promise = std::move(promise)](
                            const std::error_code &ec,
                            ServerMessagePtr protoMessage) {
            if (ec) {
                LOG(ERROR) << "Communicate error: " << ec.message() << "("
                           << ec.value() << ")";

                promise->setException(std::system_error{ec});
            }
            else {
                promise->setWith(
                    [protoMessage = std::move(protoMessage)]() mutable {
                        return SvrMsg{std::move(protoMessage)};
                    });
            }
        };

        // NOLINTNEXTLINE
        LowerLayer::communicate(messages::serialize(std::move(msg)),
            std::move(callback), retries); // NOLINT

        return future;
    }

    template <class SvrMsg, class CliMsg>
    folly::Future<SvrMsg> communicateRaw(
        CliMsg &&msg, const int retries = DEFAULT_RETRY_NUMBER)
    {
        LOG_FCALL() << LOG_FARG(retries);

        auto promise = std::make_shared<folly::Promise<SvrMsg>>();
        auto future =
            promise->getSemiFuture().via(LowerLayer::executor().get());
        auto callback = [promise = std::move(promise)](
                            const std::error_code &ec,
                            ServerMessagePtr protoMessage) {
            if (ec) {
                LOG(ERROR) << "Communicate error: " << ec.message() << "("
                           << ec.value() << ")";

                promise->setException(std::system_error{ec});
            }
            else {
                promise->setWith(
                    [protoMessage = std::move(protoMessage)]() mutable {
                        return SvrMsg{std::move(protoMessage)};
                    });
            }
        };

        // NOLINTNEXTLINE
        LowerLayer::communicate(
            std::move(msg), std::move(callback), retries); // NOLINT

        return future;
    }
};

template <class LowerLayer>
folly::Future<folly::Unit> Translator<LowerLayer>::send(
    messages::ClientMessage &&msg, const int retries)
{
    LOG_FCALL() << LOG_FARG(retries);

    LOG_DBG(4) << "Sending clproto message: {" << msg.toString() << "}";

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    auto callback = [promise](const std::error_code &ec) {
        if (ec) {
            LOG(ERROR) << "Send error: " << ec.message() << "(" << ec.value()
                       << ")";
            promise->setException(std::system_error{ec});
        }
        else
            promise->setValue();
    };

    return LowerLayer::send(
        messages::serialize(std::move(msg)), std::move(callback), retries)
        .thenValue([promise = std::move(promise)](
                       auto && /*unit*/) { return promise->getFuture(); });
}

} // namespace layers

/**
 * Waits for a future value, throwing a system_error timed_out exception if the
 * timeout has been exceeded.
 * @param msg The future to wait for.
 * @param timeout The timeout to wait for.
 * @returns The value of @c msg.get().
 */
template <class Future, typename Rep, typename Period>
decltype(auto) wait(
    Future &&future, const std::chrono::duration<Rep, Period> timeout)
{
    using namespace std::literals;
    assert(timeout > 0ms);
    return std::forward<Future>(future)
        .within(timeout,
            std::system_error{std::make_error_code(std::errc::timed_out)})
        .get();
}

} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_LAYERS_TRANSLATOR_H
