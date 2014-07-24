/**
 * @file communicator_test.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "communication/communicator.h"

#include "communication_protocol.pb.h"
#include "communication/communicationHandler.h"
#include "communication/connection.h"
#include "communication/connectionPool_mock.h"
#include "fuse_messages.pb.h"
#include "logging.pb.h"
#include "make_unique.h"
#include "remote_file_management.pb.h"
#include "testCommon.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <random>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>

using namespace ::testing;
using namespace std::placeholders;

struct CommunicationHandlerMock: public veil::communication::CommunicationHandler
{
    using Message = veil::protocol::communication_protocol::ClusterMsg;
    using Answer = veil::protocol::communication_protocol::Answer;

    bool autoFulfillPromise = true;
    std::unique_ptr<std::promise<std::unique_ptr<Answer>>> promise;

    CommunicationHandlerMock()
        : CommunicationHandler{std::make_unique<NiceMock<ConnectionPoolMock>>(),
                               std::make_unique<NiceMock<ConnectionPoolMock>>()}
    {
    }

    std::future<std::unique_ptr<Answer>> communicate(Message &msg, const Pool pool) override
    {
        promise = std::make_unique<std::promise<std::unique_ptr<Answer>>>();
        communicateMock(msg, pool);
        if(autoFulfillPromise)
        {
            auto value = std::make_unique<Answer>(Answer{});
            promise->set_value(std::move(value));
        }

        return promise->get_future();
    }

    MOCK_METHOD3(reply, void(const Answer&, Message&, const Pool));
    MOCK_METHOD2(send, void(Message&, const Pool));
    MOCK_METHOD2(communicateMock, void(Message&, const Pool));
    MOCK_METHOD1(subscribe, void(SubscriptionData));
    MOCK_METHOD2(addHandshake, void(const Message&, const Pool));
    MOCK_METHOD3(addHandshake, void(const Message&, const Message&, const Pool));
};

struct CommunicatorTest: public ::testing::Test
{
    std::unique_ptr<veil::communication::Communicator> communicator;
    CommunicationHandlerMock *handlerMock;
    std::string fuseId;

    CommunicatorTest()
    {
        fuseId = randomString();

        auto p = std::make_unique<NiceMock<CommunicationHandlerMock>>();
        handlerMock = p.get();

        communicator = std::make_unique<veil::communication::Communicator>(
                    std::move(p));

        communicator->setFuseId(fuseId);
    }
};

TEST_F(CommunicatorTest, shouldAddHandshakeAndGoodbyeOnEnablePushChannel)
{
    CommunicationHandlerMock::Message addedHandshake;
    CommunicationHandlerMock::Message addedGoodbye;

    EXPECT_CALL(*handlerMock, addHandshake(_, _, veil::communication::CommunicationHandler::Pool::META)).
            WillOnce(DoAll(SaveArg<0>(&addedHandshake), SaveArg<1>(&addedGoodbye)));

    communicator->enablePushChannel({});

    veil::protocol::fuse_messages::ChannelRegistration reg;
    veil::protocol::fuse_messages::ChannelClose close;

    ASSERT_TRUE(reg.ParseFromString(addedHandshake.input()));
    ASSERT_EQ(fuseId, reg.fuse_id());

    ASSERT_TRUE(close.ParseFromString(addedGoodbye.input()));
    ASSERT_EQ(fuseId, close.fuse_id());
}

ACTION_P2(SaveFunctions, predicate, callback)
{
    *predicate = arg0.predicate;
    *callback = arg0.callback;
}

TEST_F(CommunicatorTest, shouldSubscribeForPushMessagesOnEnablePushChannel)
{
    std::function<bool(const CommunicationHandlerMock::Answer&)> subscribedPredicate;
    std::function<bool(const CommunicationHandlerMock::Answer&)> subscribedCallback;

    EXPECT_CALL(*handlerMock, subscribe(_))
            .WillOnce(SaveFunctions(&subscribedPredicate, &subscribedCallback));

    communicator->enablePushChannel([](const CommunicationHandlerMock::Answer&){});

    std::bernoulli_distribution dis{0.5};
    CommunicationHandlerMock::Answer answer;

    for(int i = randomInt(100, 1000); i >= 0; --i)
    {
        if(dis(gen))
        {
            answer.set_message_id(randomInt(std::numeric_limits<decltype(answer.message_id())>::min(), -1));
            ASSERT_TRUE(subscribedCallback(answer));
            ASSERT_TRUE(subscribedPredicate(answer));
        }
        else
        {
            answer.set_message_id(randomInt(0, std::numeric_limits<decltype(answer.message_id())>::max()));
            ASSERT_FALSE(subscribedPredicate(answer));
        }
    }
}

TEST_F(CommunicatorTest, shouldAddHandshakeOnEnableHandshakeACK)
{
    CommunicationHandlerMock::Message addedMetaHandshake;
    CommunicationHandlerMock::Message addedDataHandshake;

    EXPECT_CALL(*handlerMock, addHandshake(_, CommunicationHandlerMock::Pool::META)).
            WillOnce(SaveArg<0>(&addedMetaHandshake));

    EXPECT_CALL(*handlerMock, addHandshake(_, CommunicationHandlerMock::Pool::DATA)).
            WillOnce(SaveArg<0>(&addedDataHandshake));

    communicator->enableHandshakeAck();

    veil::protocol::fuse_messages::HandshakeAck ack;
    ASSERT_TRUE(ack.ParseFromString(addedMetaHandshake.input()));
    ASSERT_EQ(fuseId, ack.fuse_id());

    ASSERT_TRUE(ack.ParseFromString(addedDataHandshake.input()));
    ASSERT_EQ(fuseId, ack.fuse_id());
}


TEST_F(CommunicatorTest, shouldCallDataPoolOnSendingRemoteFileManagementMessages)
{
    veil::protocol::remote_file_management::ChangePermsAtStorage msg;
    msg.set_file_id(randomString());
    msg.set_perms(666);

    EXPECT_CALL(*handlerMock, send(_, CommunicationHandlerMock::Pool::DATA));
    communicator->send(randomString(), msg);
    Mock::VerifyAndClearExpectations(handlerMock);

    EXPECT_CALL(*handlerMock, communicateMock(_, CommunicationHandlerMock::Pool::DATA));
    communicator->communicate<veil::protocol::communication_protocol::Atom>(randomString(), msg, std::chrono::milliseconds{0});
    Mock::VerifyAndClearExpectations(handlerMock);

    EXPECT_CALL(*handlerMock, communicateMock(_, CommunicationHandlerMock::Pool::DATA));
    communicator->communicateAsync<veil::protocol::communication_protocol::Atom>(randomString(), msg);
}


TEST_F(CommunicatorTest, shouldCallDataPoolOnSendingOtherMessages)
{
    veil::protocol::fuse_messages::ChannelClose msg;
    msg.set_fuse_id(fuseId);

    EXPECT_CALL(*handlerMock, send(_, CommunicationHandlerMock::Pool::META));
    communicator->send(randomString(), msg);
    Mock::VerifyAndClearExpectations(handlerMock);

    EXPECT_CALL(*handlerMock, communicateMock(_, CommunicationHandlerMock::Pool::META));
    communicator->communicate<veil::protocol::communication_protocol::Atom>(randomString(), msg, std::chrono::milliseconds{0});
    Mock::VerifyAndClearExpectations(handlerMock);

    EXPECT_CALL(*handlerMock, communicateMock(_, CommunicationHandlerMock::Pool::META));
    communicator->communicateAsync<veil::protocol::communication_protocol::Atom>(randomString(), msg);
}

TEST_F(CommunicatorTest, shouldWrapAndPassMessagesOnSend)
{
    std::string module = randomString();

    veil::protocol::remote_file_management::RemoteFileMangement msg;
    msg.set_input(randomString());
    msg.set_message_type(randomString());

    veil::protocol::communication_protocol::ClusterMsg wrapper;
    EXPECT_CALL(*handlerMock, send(_, _)).WillOnce(SaveArg<0>(&wrapper));
    communicator->send(module, msg);

    ASSERT_EQ("remotefilemangement", wrapper.message_type());
    ASSERT_EQ("remote_file_management", wrapper.message_decoder_name());
    ASSERT_EQ("atom", wrapper.answer_type());
    ASSERT_EQ("communication_protocol", wrapper.answer_decoder_name());
    ASSERT_EQ(module, wrapper.module_name());
    ASSERT_FALSE(wrapper.synch());
    ASSERT_TRUE(wrapper.has_protocol_version());

    decltype(msg) sentMsg;
    ASSERT_TRUE(sentMsg.ParseFromString(wrapper.input()));
    ASSERT_EQ(msg.SerializeAsString(), sentMsg.SerializeAsString());
}

TEST_F(CommunicatorTest, shouldWrapAndPassMessagesOnCommunicate)
{
    std::string module = randomString();

    veil::protocol::fuse_messages::CreateDir msg;
    msg.set_dir_logic_name(randomString());
    msg.set_mode(666);

    veil::protocol::communication_protocol::ClusterMsg wrapper;
    EXPECT_CALL(*handlerMock, communicateMock(_, _)).WillOnce(SaveArg<0>(&wrapper));
    communicator->communicate<veil::protocol::fuse_messages::ChannelClose>(module, msg, std::chrono::milliseconds{0});

    ASSERT_EQ("createdir", wrapper.message_type());
    ASSERT_EQ("fuse_messages", wrapper.message_decoder_name());
    ASSERT_EQ("channelclose", wrapper.answer_type());
    ASSERT_EQ("fuse_messages", wrapper.answer_decoder_name());
    ASSERT_EQ(module, wrapper.module_name());
    ASSERT_TRUE(wrapper.synch());
    ASSERT_TRUE(wrapper.has_protocol_version());

    decltype(msg) sentMsg;
    ASSERT_TRUE(sentMsg.ParseFromString(wrapper.input()));
    ASSERT_EQ(msg.SerializeAsString(), sentMsg.SerializeAsString());
}

TEST_F(CommunicatorTest, shouldWrapAndPassMessagesOnCommunicateAsync)
{
    std::string module = randomString();

    veil::protocol::logging::ChangeRemoteLogLevel msg;
    msg.set_level(veil::protocol::logging::INFO);

    veil::protocol::communication_protocol::ClusterMsg wrapper;
    EXPECT_CALL(*handlerMock, communicateMock(_, _)).WillOnce(SaveArg<0>(&wrapper));
    communicator->communicateAsync<veil::protocol::remote_file_management::DeleteFileAtStorage>(module, msg);

    ASSERT_EQ("changeremoteloglevel", wrapper.message_type());
    ASSERT_EQ("logging", wrapper.message_decoder_name());
    ASSERT_EQ("deletefileatstorage", wrapper.answer_type());
    ASSERT_EQ("remote_file_management", wrapper.answer_decoder_name());
    ASSERT_EQ(module, wrapper.module_name());
    ASSERT_FALSE(wrapper.synch());
    ASSERT_TRUE(wrapper.has_protocol_version());

    decltype(msg) sentMsg;
    ASSERT_TRUE(sentMsg.ParseFromString(wrapper.input()));
    ASSERT_EQ(msg.SerializeAsString(), sentMsg.SerializeAsString());
}

TEST_F(CommunicatorTest, shouldAskForAtomAnswerByDefault)
{
    veil::protocol::fuse_messages::ChannelRegistration msg;
    msg.set_fuse_id(fuseId);

    veil::protocol::communication_protocol::ClusterMsg wrapper;
    EXPECT_CALL(*handlerMock, communicateMock(_, _)).WillRepeatedly(SaveArg<0>(&wrapper));

    communicator->communicate(randomString(), msg);
    ASSERT_EQ("atom", wrapper.answer_type());
    ASSERT_EQ("communication_protocol", wrapper.answer_decoder_name());

    communicator->communicateAsync(randomString(), msg);
    ASSERT_EQ("atom", wrapper.answer_type());
    ASSERT_EQ("communication_protocol", wrapper.answer_decoder_name());
}

TEST_F(CommunicatorTest, shouldWaitForAnswerOnCommunicate)
{
    handlerMock->autoFulfillPromise = false;
    std::atomic<bool> communicationDone{false};
    std::condition_variable statusChanged;

    auto fulfilPromise = [&]{
        std::this_thread::sleep_for(std::chrono::milliseconds{250});
        ASSERT_FALSE(communicationDone);
        handlerMock->promise->set_value({});

        std::mutex m;
        std::unique_lock<std::mutex> lock{m};
        statusChanged.wait_for(lock, std::chrono::seconds{5}, [&]{ return communicationDone.load(); });
        ASSERT_TRUE(communicationDone);
    };

    veil::protocol::fuse_messages::ChannelRegistration msg;
    msg.set_fuse_id(fuseId);

    std::thread t{fulfilPromise};
    communicator->communicate(randomString(), msg, std::chrono::seconds{20});
    communicationDone = true;
    statusChanged.notify_one();

    t.join();
}

TEST_F(CommunicatorTest, shouldReturnEmptyMessageOnCommunicateFailure)
{
    handlerMock->autoFulfillPromise = false;

    veil::protocol::fuse_messages::ChannelRegistration msg;
    msg.set_fuse_id(fuseId);
    auto ans = communicator->communicate(randomString(), msg, std::chrono::seconds{0});

    ASSERT_FALSE(ans->has_answer_status());
    ASSERT_FALSE(ans->has_error_description());
    ASSERT_FALSE(ans->has_message_id());
    ASSERT_FALSE(ans->has_message_type());
    ASSERT_FALSE(ans->has_worker_answer());
}

TEST_F(CommunicatorTest, shouldReturnAFullfilableFutureOnCommunicateAsync)
{
    handlerMock->autoFulfillPromise = false;

    veil::protocol::fuse_messages::ChannelRegistration msg;
    msg.set_fuse_id(fuseId);
    auto future = communicator->communicateAsync(randomString(), msg);

    ASSERT_EQ(std::future_status::timeout, future.wait_for(std::chrono::seconds{0}));
    handlerMock->promise->set_value({});
    ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds{0}));
}

TEST_F(CommunicatorTest, shouldWrapAndPassMessagesOnReply)
{
    std::string module = randomString();

    veil::protocol::fuse_messages::ChannelClose msg;
    msg.set_fuse_id(fuseId);

    veil::protocol::communication_protocol::Answer replyTo;
    replyTo.set_message_id(randomInt());

    veil::protocol::communication_protocol::ClusterMsg wrapper;
    EXPECT_CALL(*handlerMock, reply(_, _, _)).WillOnce(SaveArg<1>(&wrapper));
    communicator->reply(replyTo, module, msg);

    ASSERT_EQ("channelclose", wrapper.message_type());
    ASSERT_EQ("fuse_messages", wrapper.message_decoder_name());
    ASSERT_EQ("atom", wrapper.answer_type());
    ASSERT_EQ("communication_protocol", wrapper.answer_decoder_name());
    ASSERT_EQ(module, wrapper.module_name());
    ASSERT_FALSE(wrapper.synch());
    ASSERT_TRUE(wrapper.has_protocol_version());

    decltype(msg) sentMsg;
    ASSERT_TRUE(sentMsg.ParseFromString(wrapper.input()));
    ASSERT_EQ(msg.SerializeAsString(), sentMsg.SerializeAsString());
}
