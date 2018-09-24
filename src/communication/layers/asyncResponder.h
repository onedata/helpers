/**
 * @file asyncResponder.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_COMMUNICATION_LAYERS_ASYNC_RESPONDER_H
#define HELPERS_COMMUNICATION_LAYERS_ASYNC_RESPONDER_H

#include "communication/declarations.h"
#include "logging.h"

#include <folly/ThreadName.h>
#include <folly/executors/GlobalExecutor.h>

#include <functional>
#include <memory>

namespace one {
namespace communication {
namespace layers {

/**
 * @c AsyncResponder is responsible for offloading response callbacks to
 * separate threads.
 */
template <class LowerLayer> class AsyncResponder : public LowerLayer {
public:
    using Callback = typename LowerLayer::Callback;
    using LowerLayer::LowerLayer;

    virtual ~AsyncResponder();

    /**
     * A reference to @c *this typed as a @c AsyncResponder.
     */
    AsyncResponder<LowerLayer> &asyncResponder = *this;

    /*
     * Wraps lower layer's @c connect.
     * Starts the thread that will run onMessage callbacks.
     * @see ConnectionPool::connect()
     */
    auto connect();

    /**
     * Runs higher layer's message callback in a separate thread.
     * @see ConnectionPool::setOnMessageCallback()
     */
    auto setOnMessageCallback(
        std::function<void(ServerMessagePtr)> onMessageCallback);
};

template <class LowerLayer> AsyncResponder<LowerLayer>::~AsyncResponder() {}

template <class LowerLayer> auto AsyncResponder<LowerLayer>::connect()
{
    return LowerLayer::connect();
}

template <class LowerLayer>
auto AsyncResponder<LowerLayer>::setOnMessageCallback(
    std::function<void(ServerMessagePtr)> onMessageCallback)
{
    return LowerLayer::setOnMessageCallback([onMessageCallback =
                                                 std::move(onMessageCallback)](
        ServerMessagePtr serverMsg) mutable {
        folly::getIOExecutor()->add(
            [&, serverMsg = std::move(serverMsg) ]() mutable {
                onMessageCallback(std::move(serverMsg));
            });
    });
}

} // namespace layers
} // namespace communication
} // namespace one

#endif // HELPERS_COMMUNICATION_LAYERS_ASYNC_RESPONDER_H
