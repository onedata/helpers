/**
 * @file communicator_test.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "communication/communicator.h"
#include "messages/ping.h"
#include "messages/pong.h"
#include "scheduler_mock.h"

#include <folly/executors/GlobalExecutor.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>

using namespace one;
using namespace one::communication;
using namespace std::literals::chrono_literals;

class LazyConnectionPool {
public:
    using Callback = std::function<void(const std::error_code &)>;

    enum class State { INVALID_PROVIDER };

    void setConnectionState(State s) { }
    void connect() { }

    bool isConnected() { return true; }

    std::shared_ptr<folly::Executor> executor()
    {
        return folly::getIOExecutor();
    }

    void setOnMessageCallback(std::function<void(std::string)>) { }

    void setCertificateData(std::shared_ptr<cert::CertificateData>) { }

    folly::Future<folly::Unit> send(
        std::string, Callback /*callback*/, const int = int{})
    {
        return folly::makeFuture();
    }
};

using CustomCommunicator = layers::Translator<layers::Replier<
    layers::Inbox<layers::Sequencer<layers::BinaryTranslator<layers::Logger<
                                        layers::Retrier<LazyConnectionPool>>>,
        MockScheduler>>>>;

struct CommunicatorTest : public ::testing::Test {
    CustomCommunicator comm;
    std::shared_ptr<MockScheduler> scheduler;

    CommunicatorTest()
    {
        scheduler = std::make_shared<MockScheduler>();
        comm.setScheduler(scheduler);
        comm.connect();
    }
};

TEST_F(CommunicatorTest, communicateShouldReturnFuture)
{
    folly::Future<messages::Pong> future =
        comm.communicate<messages::Pong>(messages::Ping{});
}
