/**
 * @file typedStream.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_STREAMING_TYPED_STREAM_H
#define HELPERS_COMMUNICATION_STREAMING_TYPED_STREAM_H

#include "communication/declarations.h"
#include "messages/clientMessage.h"
#include "messages/endOfStream.h"
#include "messages/status.h"

#include <tbb/concurrent_priority_queue.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <vector>
/**
 * std::<shared_timed_mutex> on OSX is available only since
 * macOS Sierra (10.12), so use folly::SharedMutex on older OSX version.
 */
#if defined(__APPLE__) && (MAC_OS_X_VERSION_MAX_ALLOWED < 101200)
#include <folly/SharedMutex.h>
#endif

namespace one {
namespace communication {
namespace streaming {

#if defined(__APPLE__) && (MAC_OS_X_VERSION_MAX_ALLOWED < 101200)
using BufferMutexType = folly::SharedMutex;
#else
using BufferMutexType = std::shared_timed_mutex;
#endif

struct StreamLess {
    bool operator()(const ClientMessagePtr &a, const ClientMessagePtr &b) const
    {
        return a->message_stream().sequence_number() >
            b->message_stream().sequence_number();
    }
};

/**
 * @c TypedStream provides outgoing message stream functionalities to an
 * existing communication stack.
 */
template <class Communicator> class TypedStream {
public:
    /**
     * Constructor.
     * @param communicator The communication stack used to send stream messages.
     * It must at least implement @c send(ClientMessagePtr, const int).
     * @param streamId ID number of this stream.
     */
    TypedStream(
        std::shared_ptr<Communicator> communicator, std::uint64_t streamId,
        std::function<void()> unregister = [] {});

    /**
     * Destructor.
     * Closes the stream if not yet closed.
     */
    virtual ~TypedStream();

    /**
     * Sends a next message in the stream.
     * @param msg The message to send through the stream.
     */
    virtual void send(messages::ClientMessage &&msg);

    /**
     * Sends a next message in the stream.
     * @param msg The message to send through the stream.
     */
    virtual void send(ClientMessagePtr msg);

    virtual void sendSync(ClientMessagePtr msg);

    /**
     * Resends messages requested by the remote party.
     * @param msg Details of the request.
     */
    void handleMessageRequest(const clproto::MessageRequest &msg);

    /**
     * Removes already processed messages from an internal buffer.
     * @param msg Details of processed messages.
     */
    void handleMessageAcknowledgement(
        const clproto::MessageAcknowledgement &msg);

    /**
     * Closes the stream by sending an end-of-stream message.
     */
    virtual void close();

    /**
     * Resets the stream's counters and resends all messages with recomputed
     * sequence number.
     */
    void reset();

    TypedStream(TypedStream &&) = delete;
    TypedStream(const TypedStream &) = delete;
    TypedStream &operator=(TypedStream &&) = delete;
    TypedStream &operator=(const TypedStream) = delete;

private:
    void saveAndPass(ClientMessagePtr msg);
    void saveAndPassSync(ClientMessagePtr msg);

    std::shared_ptr<Communicator> m_communicator;
    const std::uint64_t m_streamId;
    std::function<void()> m_unregister;
    std::atomic<std::uint64_t> m_sequenceId{0};
    BufferMutexType m_bufferMutex;
    tbb::concurrent_priority_queue<ClientMessagePtr, StreamLess> m_buffer;
};

template <class Communicator>
TypedStream<Communicator>::TypedStream(
    std::shared_ptr<Communicator> communicator, const uint64_t streamId,
    std::function<void()> unregister)
    : m_communicator{std::move(communicator)}
    , m_streamId{streamId}
    , m_unregister{std::move(unregister)}
{
    LOG_FCALL() << LOG_FARG(streamId);
}

template <class Communicator> TypedStream<Communicator>::~TypedStream()
{
    LOG_FCALL();

    close();
    m_unregister();
}

template <class Communicator>
void TypedStream<Communicator>::send(messages::ClientMessage &&msg)
{
    LOG_FCALL() << LOG_FARG(msg.toString());

    send(messages::serialize(std::move(msg)));
}

template <class Communicator>
void TypedStream<Communicator>::send(ClientMessagePtr msg)
{
    LOG_FCALL();

    auto *msgStream = msg->mutable_message_stream();
    msgStream->set_stream_id(m_streamId);
    msgStream->set_sequence_number(m_sequenceId++);
    saveAndPass(std::move(msg));
}

template <class Communicator>
void TypedStream<Communicator>::sendSync(ClientMessagePtr msg)
{
    LOG_FCALL();

    auto *msgStream = msg->mutable_message_stream();
    msgStream->set_stream_id(m_streamId);
    msgStream->set_sequence_number(m_sequenceId++);
    saveAndPassSync(std::move(msg));
}

template <class Communicator> void TypedStream<Communicator>::close()
{
    LOG_FCALL();

    send(messages::EndOfStream{});
}

template <class Communicator> void TypedStream<Communicator>::reset()
{
    LOG_FCALL();

    LOG_DBG(3) << "Renumbering stream message " << m_streamId
               << " sequence numbers from 0";

    std::lock_guard<BufferMutexType> lock{m_bufferMutex};
    m_sequenceId = 0;
    std::vector<ClientMessagePtr> processed;
    for (ClientMessagePtr it; m_buffer.try_pop(it);) {
        LOG_DBG(3) << "Resetting stream message sequence number to: "
                   << m_sequenceId;
        it->mutable_message_stream()->set_sequence_number(m_sequenceId++);
        processed.emplace_back(std::move(it));
    }
    for (auto &msgStream : processed) {
        auto msgCopy = std::make_unique<clproto::ClientMessage>(*msgStream);
        m_buffer.emplace(std::move(msgStream));
    }
}

template <class Communicator>
void TypedStream<Communicator>::saveAndPass(ClientMessagePtr msg)
{
    LOG_FCALL();

    if (m_communicator->isConnected()) {
        auto msgCopy = std::make_unique<clproto::ClientMessage>(*msg);

        {
            std::shared_lock<BufferMutexType> lock{m_bufferMutex};
            m_buffer.emplace(std::move(msgCopy));
        }

        m_communicator->send(
            std::move(msg), [](auto /*unused*/) {}, 0);
    }
    else
        LOG_DBG(1) << "Connection is down - skipped sending typed message";
}

template <class Communicator>
void TypedStream<Communicator>::saveAndPassSync(ClientMessagePtr msg)
{
    LOG_FCALL();

    if (m_communicator->isConnected()) {
        auto msgCopy = std::make_unique<clproto::ClientMessage>(*msg);

        {
            std::shared_lock<BufferMutexType> lock{m_bufferMutex};
            m_buffer.emplace(std::move(msgCopy));
        }

        m_communicator->template communicateRaw<messages::Status>(
            std::move(msg), 0);
    }
    else
        LOG_DBG(1) << "Connection is down - skipped sending typed message";
}

template <class Communicator>
void TypedStream<Communicator>::handleMessageRequest(
    const clproto::MessageRequest &msg)
{
    LOG_FCALL();

    LOG_DBG(4) << "Oneprovider requested messages in stream: "
               << msg.stream_id() << " in range: ("
               << msg.lower_sequence_number() << ", "
               << msg.upper_sequence_number() << ")";

    std::vector<ClientMessagePtr> processed;
    processed.reserve(
        msg.upper_sequence_number() - msg.lower_sequence_number() + 1);

    std::shared_lock<BufferMutexType> lock{m_bufferMutex};
    for (ClientMessagePtr it; m_buffer.try_pop(it);) {
        if (it->message_stream().sequence_number() <=
            msg.upper_sequence_number()) {
            LOG_DBG(4) << "Found requested message with sequence number: "
                       << it->message_stream().sequence_number();

            processed.emplace_back(std::move(it));
        }
        else {
            LOG_DBG(4) << "Putting back message with sequence number: "
                       << it->message_stream().sequence_number();

            m_buffer.emplace(std::move(it));
            break;
        }
    }

    for (auto &streamMessage : processed) {
        if (streamMessage->message_stream().sequence_number() >=
            msg.lower_sequence_number()) {
            LOG_DBG(4) << "Sending requested stream " << msg.stream_id()
                       << "  message "
                       << streamMessage->message_stream().sequence_number();

            saveAndPass(std::move(streamMessage));
        }
        else {
            LOG_DBG(4) << "Putting back message: "
                       << streamMessage->message_stream().sequence_number();

            m_buffer.emplace(std::move(streamMessage));
        }
    }
}

template <class Communicator>
void TypedStream<Communicator>::handleMessageAcknowledgement(
    const clproto::MessageAcknowledgement &msg)
{
    LOG_FCALL();

    LOG_DBG(3) << "Received message acknowledgement for stream id: "
               << msg.stream_id()
               << " sequence number: " << msg.sequence_number();

    std::shared_lock<BufferMutexType> lock{m_bufferMutex};
    for (ClientMessagePtr it; m_buffer.try_pop(it);) {
        // Keep the message in the buffer if it's sequence number is larger than
        // the acknowledgement number
        if (it->message_stream().sequence_number() > msg.sequence_number()) {
            m_buffer.emplace(std::move(it));
            break;
        }
        LOG_DBG(3) << "Dropped acknowledged stream " << msg.stream_id()
                   << " message with sequence number "
                   << it->message_stream().sequence_number();
    }
}

} // namespace streaming
} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_STREAMING_TYPED_STREAM_H
