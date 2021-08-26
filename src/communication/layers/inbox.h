/**
 * @file inbox.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_LAYERS_INBOX_H
#define HELPERS_COMMUNICATION_LAYERS_INBOX_H

#include "communication/declarations.h"
#include "communication/subscriptionData.h"
#include "helpers/logging.h"

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <system_error>

namespace {
inline std::uint64_t initializeMsgIdSeed()
{
    auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    return time.count();
}
} // namespace

namespace one {
namespace communication {
namespace layers {

constexpr auto kMagicCookie = 1234432178;

/**
 * @c Inbox is responsible for handling incoming messages. It stores a
 * collection of unfulfilled promises and matches them to incoming messages
 * by a message id. Other objects can subscribe on messages, e.g. to create
 * a 'push message' communication channel.
 */
template <class LowerLayer>
class Inbox : public LowerLayer,
              public std::enable_shared_from_this<Inbox<LowerLayer>> {
public:
    using Callback = typename LowerLayer::Callback;
    using CommunicateCallback =
        std::function<void(const std::error_code &ec, ServerMessagePtr)>;

    using LowerLayer::LowerLayer;
    ~Inbox() override = default;

    /**
     * A reference to @c *this typed as an @c Inbox.
     */
    Inbox<LowerLayer> &inbox = *this; // NOLINT

    /**
     * Sends a message to the server and sets up to receive a reply.
     * @param message The message to be sent.
     * @param retries Number of retries in case of sending error.
     * @return A future which should be fulfiled with server's reply.
     */
    void communicate(ClientMessagePtr message, CommunicateCallback callback,
        int retries = DEFAULT_RETRY_NUMBER);

    /**
     * Subscribes a given callback to messages received from the server.
     * Message can be received through any connection.
     * The @c SubscriptionData::predicate is first called to determine if
     * the @c SubscriptionData::callback should be called.
     * @param data A structure holding the predicate and callback functions.
     * @return A function to cancel the subscription.
     * @note Subscription callbacks should be lightweight, as they are handled
     * in communicator's threads.
     */
    std::function<void()> subscribe(SubscriptionData data);

    /**
     * Wraps lower layer's @c connect.
     * Sets a custom @c setOnMessageCallback on all lower layers. This layer
     * does not provide a @c setOnMessageCallback method.
     * @see ConnectionPool::connect()
     */
    auto connect();

    void setOnMessageCallback(std::function<void(ServerMessagePtr)>) = delete;

    void stop()
    {
        m_stopped = true;
        LowerLayer::stop();
    }

private:
    struct CommunicateCallbackData {
        std::shared_ptr<CommunicateCallback> callback;
        std::chrono::time_point<std::chrono::system_clock> sendTime;
        std::string messageName;
    };

    tbb::concurrent_hash_map<std::string, CommunicateCallbackData> m_callbacks;

    std::uint64_t m_seed = initializeMsgIdSeed();
    /// The counter will loop after sending ~65000 messages, providing us with
    /// a natural size bound for m_callbacks.
    std::atomic<std::uint16_t> m_nextMsgId{0};

    tbb::concurrent_vector<SubscriptionData> m_subscriptions;
    tbb::concurrent_queue<typename decltype(m_subscriptions)::iterator>
        m_unusedSubscriptions;

    bool m_stopped = false;

    // This is a temporary way of making sure that the communicator instance
    // is still valid after deamonization from within a callback having
    // only this pointer. This should be fixed with a proper lifetime management
    // during communicator destruction
    int m_magic = kMagicCookie;
};

template <class LowerLayer>
void Inbox<LowerLayer>::communicate(
    ClientMessagePtr message, CommunicateCallback callback, const int retries)
{
    const auto messageId = std::to_string(m_seed + m_nextMsgId++);
    message->set_message_id(messageId);

    {
        typename decltype(m_callbacks)::accessor acc;
        m_callbacks.insert(acc, messageId);

        std::string messageName{"client_message"};

        if (VLOG_IS_ON(3)) {
            if (message->has_fuse_request()) {
                auto fuseRequest = message->fuse_request();
                if (fuseRequest.has_file_request()) {
                    const auto &fileRequest = fuseRequest.file_request();
                    auto fileRequestCase = fileRequest.file_request_case();
                    messageName = "fuse_request.file_request." +
                        fileRequest.GetDescriptor()
                            ->FindOneofByName("file_request")
                            ->field(fileRequestCase -
                                one::clproto::FileRequest::FileRequestCase::
                                    kGetFileAttr)
                            ->name() +
                        " [" + std::to_string(fileRequestCase) + "]";
                }
            }
            else if (message->GetDescriptor()->FindOneofByName(
                         "message_body") != nullptr) {
                auto messageBodyCase = message->message_body_case();
                if (messageBodyCase != 0U) {
                    messageName =
                        message->GetDescriptor()
                            ->FindOneofByName("message_body")
                            ->field(messageBodyCase -
                                one::clproto::ClientMessage::MessageBodyCase::
                                    kClientHandshakeRequest)
                            ->name() +
                        " [" + std::to_string(message->message_body_case()) +
                        "]";
                }
            }
        }

        acc->second = CommunicateCallbackData{
            std::make_shared<CommunicateCallback>(std::move(callback)),
            std::chrono::system_clock::now(), messageName};
    }

    auto sendErrorCallback = [this, messageId = messageId](
                                 const std::error_code &ec) {
        if (ec) {
            typename decltype(m_callbacks)::accessor acc;
            if (m_callbacks.find(acc, messageId)) {
                auto cb = std::move(*(acc->second.callback));
                auto messageName = std::move(acc->second.messageName);
                namespace sc = std::chrono;
                auto rtt = sc::duration_cast<sc::milliseconds>(
                    sc::system_clock::now() - acc->second.sendTime);

                LOG(WARNING) << "Sending message " << messageName
                             << "(id: " << messageId << ") failed after "
                             << rtt.count() << "ms with error: " << ec;

                m_callbacks.erase(acc);
                cb(ec, {});
            }
        }
    };

    LowerLayer::send(std::move(message), std::move(sendErrorCallback), retries);
}

template <class LowerLayer>
std::function<void()> Inbox<LowerLayer>::subscribe(SubscriptionData data)
{
    typename decltype(m_subscriptions)::iterator it;
    if (m_unusedSubscriptions.try_pop(it))
        *it = std::move(data);
    else
        it = m_subscriptions.emplace_back(std::move(data));

    return [this, it] {
        *it = SubscriptionData{};
        m_unusedSubscriptions.push(it);
    };
}

template <class LowerLayer> auto Inbox<LowerLayer>::connect()
{
    LowerLayer::setOnMessageCallback([this](ServerMessagePtr message) {
        if (m_magic != kMagicCookie) {
            LOG(INFO) << "Received message but communicator is already "
                         "destroyed - ignoring...";
            return;
        }

        if (m_stopped) {
            LOG(INFO) << "Received message but communicator already stopped - "
                         "ignoring...";
            return;
        }

        const auto messageId = message->message_id();

        typename decltype(m_callbacks)::accessor acc;
        const bool handled = m_callbacks.find(acc, messageId);

        for (const auto &sub : m_subscriptions)
            if (sub.predicate()(*message, handled))
                sub.callback()(*message);

        if (handled) {
            auto callback = std::move(*(acc->second.callback));

            if (VLOG_IS_ON(3)) {
                auto messageName = std::move(acc->second.messageName);
                using namespace std::chrono;
                auto rtt = duration_cast<milliseconds>(
                    system_clock::now() - acc->second.sendTime);

                LOG_DBG(3) << "Response to message " << messageName
                           << "(id: " << messageId << ") received in "
                           << rtt.count() << " ms";
            }

            m_callbacks.erase(acc);
            callback({}, std::move(message));
        }
    });

    return LowerLayer::connect();
}

} // namespace layers
} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_LAYERS_INBOX_H
