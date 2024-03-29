/**
 * @file streamManager.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_STREAMING_STREAM_MANAGER_H
#define HELPERS_COMMUNICATION_STREAMING_STREAM_MANAGER_H

#include "communication/layers/translator.h"
#include "communication/subscriptionData.h"
#include "typedStream.h"

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace one {
namespace communication {
namespace streaming {

template <class Communicator> class StreamManager {
public:
    using Stream = ::one::communication::streaming::TypedStream<Communicator>;

    /**
     * Constructor.
     * @param communicator The communicator stack used with created streams,
     * and for subscription to incoming messages.
     * @todo Communicator as a reference
     */
    explicit StreamManager(std::shared_ptr<Communicator> communicator,
        std::chrono::seconds providerTimeout);

    /**
     * Creates a new @c Stream.
     * The returned stream object must be destroyed before @c this is destroyed.
     * @return A new stream instance.
     */
    std::shared_ptr<Stream> create();

    /**
     * Reset all streams.
     */
    void reset();

private:
    void handleMessageRequest(const clproto::MessageRequest &msg);
    void handleMessageAcknowledgement(
        const clproto::MessageAcknowledgement &msg);
    void handleMessageStreamReset(const clproto::MessageStreamReset &msg);

    std::function<void()> m_unsubscribe;
    std::shared_ptr<Communicator> m_communicator;
    std::atomic<std::uint64_t> m_nextStreamId{0};

    tbb::concurrent_hash_map<std::uint64_t, std::weak_ptr<Stream>> m_idMap;
    tbb::concurrent_vector<std::weak_ptr<Stream>> m_streams;
    tbb::concurrent_queue<typename decltype(m_streams)::iterator>
        m_streamsSlots;
    const std::chrono::seconds m_providerTimeout;
};

template <class Communicator>
StreamManager<Communicator>::StreamManager(
    std::shared_ptr<Communicator> communicator,
    const std::chrono::seconds providerTimeout)
    : m_communicator{std::move(communicator)}
    , m_providerTimeout{providerTimeout}
{
    auto predicate = [](const clproto::ServerMessage &msg,
                         const bool /*unused*/) {
        return msg.has_message_request() || msg.has_message_acknowledgement() ||
            msg.has_message_stream_reset();
    };

    auto callback = [this](const clproto::ServerMessage &msg) {
        if (msg.has_message_request())
            handleMessageRequest(msg.message_request());
        else if (msg.has_message_acknowledgement())
            handleMessageAcknowledgement(msg.message_acknowledgement());
        else if (msg.has_message_stream_reset()) {
            handleMessageStreamReset(msg.message_stream_reset());
        }
    };

    m_unsubscribe = m_communicator->subscribe(
        SubscriptionData{std::move(predicate), std::move(callback)});
}

template <class Communicator>
auto StreamManager<Communicator>::create() -> std::shared_ptr<Stream>
{
    typename decltype(m_streams)::iterator it;
    if (!m_streamsSlots.try_pop(it))
        it = m_streams.emplace_back();

    std::uint64_t streamId = m_nextStreamId++;
    auto stream = std::make_shared<Stream>(
        m_communicator, streamId, m_providerTimeout, [this, streamId, it] {
            m_idMap.erase(streamId);
            m_streamsSlots.emplace(std::move(it));
        });

    *it = stream;

    typename decltype(m_idMap)::accessor acc;
    m_idMap.insert(acc, streamId);
    acc->second = *it;

    LOG_DBG(2) << "Created stream with stream id " << streamId;

    return stream;
}

template <class Communicator>
void StreamManager<Communicator>::handleMessageRequest(
    const clproto::MessageRequest &msg)
{
    LOG_FCALL() << LOG_FARG(msg.stream_id());

    typename decltype(m_idMap)::const_accessor acc;
    if (m_idMap.find(acc, msg.stream_id())) {
        if (auto stream = acc->second.lock())
            stream->handleMessageRequest(msg);
        else
            LOG(ERROR) << "Cannot handle message request for stream id: "
                       << msg.stream_id() << " - cannot lock stream pointer...";
    }
    else {
        LOG(ERROR) << "Cannot handle message request for stream id: "
                   << msg.stream_id();
    }
}

template <class Communicator>
void StreamManager<Communicator>::handleMessageAcknowledgement(
    const clproto::MessageAcknowledgement &msg)
{
    LOG_FCALL() << LOG_FARG(msg.stream_id());

    typename decltype(m_idMap)::const_accessor acc;
    if (m_idMap.find(acc, msg.stream_id()))
        if (auto stream = acc->second.lock())
            stream->handleMessageAcknowledgement(msg);
}

template <class Communicator>
void StreamManager<Communicator>::handleMessageStreamReset(
    const clproto::MessageStreamReset &msg)
{
    LOG_FCALL();

    if (msg.has_stream_id()) {
        LOG_DBG(3) << "Resetting stream with stream id " << msg.stream_id();
        typename decltype(m_idMap)::const_accessor acc;
        if (m_idMap.find(acc, msg.stream_id()))
            if (auto stream = acc->second.lock())
                stream->reset();
    }
    else {
        LOG_DBG(3)
            << "No stream id in stream reset message - resetting all streams";
        reset();
    }
}

template <class Communicator> void StreamManager<Communicator>::reset()
{
    for (const auto &stream : m_streams)
        if (auto s = stream.lock())
            s->reset();
}

} // namespace streaming
} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_STREAMING_STREAM_MANAGER_H
