#include "communication/communicator.h"
#include "communication/declarations.h"
#include "helpers/init.h"
#include "messages/clientHandshakeRequest.h"
#include "messages/clientMessage.h"
#include "messages/serverMessage.h"

#include "messages.pb.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/smart_ptr.hpp>
#include <glog/logging.h>

#include <chrono>
#include <memory>
#include <string>

using namespace std::literals;
using namespace one::communication;
using namespace one;
using namespace boost::python;
using namespace one::messages;

template <class LowerLayer> class Hijacker : public LowerLayer {
public:
    using Callback = typename LowerLayer::Callback;
    using LowerLayer::LowerLayer;

    auto send(std::string msg, Callback callback, const int retry)
    {
        m_lastMessageSent = msg;
        return LowerLayer::send(std::move(msg), std::move(callback), retry);
    }

    std::string &lastMessageSent() { return m_lastMessageSent; }

    auto setHandshake(std::function<std::string()> getHandshake,
        std::function<std::error_code(std::string)> onHandshakeResponse,
        std::function<void(std::error_code)> onHandshakeDone)
    {
        m_handshake = getHandshake();
        return LowerLayer::setHandshake(
            std::move(getHandshake),
            [this, onHandshakeResponse = std::move(onHandshakeResponse)](
                std::string response) {
                try {
                    m_handshakeResponsePromise.set_value(response);
                }
                catch (std::future_error &) {
                }
                return onHandshakeResponse(std::move(response));
            },
            [](auto) {});
    }

    std::string &handshake() { return m_handshake; }

    std::string handshakeResponse()
    {
        return m_handshakeResponsePromise.get_future().get();
    }

private:
    std::promise<std::string> m_handshakeResponsePromise;
    std::string m_handshake;
    std::string m_lastMessageSent;
};

using CustomCommunicator = layers::Translator<
    layers::Replier<layers::Inbox<layers::Sequencer<layers::BinaryTranslator<
        layers::Logger<Hijacker<layers::Retrier<ConnectionPool>>>>>>>>;

class ExampleClientMessage : public messages::ClientMessage {
public:
    ExampleClientMessage(std::string description)
        : m_description{std::move(description)}
    {
    }

    std::string toString() const override { return ""; }

    std::unique_ptr<ProtocolClientMessage> serializeAndDestroy() override
    {
        auto msg = std::make_unique<ProtocolClientMessage>();
        auto status = msg->mutable_status();
        status->set_code(one::clproto::Status_Code_ok);
        status->set_description(m_description);
        return msg;
    }

private:
    std::string m_description;
};

class ExampleServerMessage : public messages::ServerMessage {
public:
    ExampleServerMessage(std::unique_ptr<ProtocolServerMessage> protocolMsg_)
        : m_protocolMsg{std::move(protocolMsg_)}
    {
    }

    virtual std::string toString() const override { return ""; }

    ProtocolServerMessage &protocolMsg() const { return *m_protocolMsg; }

private:
    std::unique_ptr<ProtocolServerMessage> m_protocolMsg;
};

class CommunicatorProxy {
public:
    CommunicatorProxy(const std::size_t connectionsNumber,
        std::size_t workersNumber, std::string host, const unsigned short port,
        bool handshake)
        : m_communicator{connectionsNumber, workersNumber, host, port, false,
              true, handshake}
    {
        m_communicator.setScheduler(std::make_shared<Scheduler>(1));
    }

    ~CommunicatorProxy() { stop(); }

    void stop()
    {
        if (!m_stopped.test_and_set()) {
            m_communicator.stop();
        }
    }

    void connect() { m_communicator.connect(); }

    std::string send(const std::string &description)
    {
        m_communicator.send(ExampleClientMessage{description});
        return m_communicator.lastMessageSent();
    }

    void sendAsync(const std::string &description)
    {
        std::thread{[=] { send(description); }}.detach();
    }

    std::string communicate(const std::string &description)
    {
        m_future = m_communicator.communicate<ExampleServerMessage>(
            ExampleClientMessage{description}, int{});

        return m_communicator.lastMessageSent();
    }

    std::string communicateReceive()
    {
        return std::move(*m_future)
            .within(10s,
                std::system_error{std::make_error_code(std::errc::timed_out)})
            .get()
            .protocolMsg()
            .SerializeAsString();
    }

    std::string setHandshake(const std::string &description, bool fail)
    {
        m_communicator.setHandshake(
            [=] { return messages::ClientHandshakeRequest{description}; },
            [=](auto) {
                return fail ? std::make_error_code(std::errc::bad_message)
                            : std::error_code{};
            });

        return m_communicator.handshake();
    }

    std::string handshakeResponse()
    {
        return m_communicator.handshakeResponse();
    }

private:
    CustomCommunicator m_communicator;
    folly::Optional<folly::Future<ExampleServerMessage>> m_future;
    std::atomic_flag m_stopped = ATOMIC_FLAG_INIT;
};

boost::shared_ptr<CommunicatorProxy> create(
    const unsigned int connectionsNumber, const unsigned int workersNumber,
    std::string host, const unsigned short port, const bool handshake)
{
    FLAGS_v = 0;

    helpers::init();

    return boost::make_shared<CommunicatorProxy>(
        connectionsNumber, workersNumber, std::move(host), port, handshake);
}

std::string prepareReply(
    const std::string &toWhat, const std::string &description)
{
    one::clproto::ClientMessage clientMsg;
    clientMsg.ParseFromString(toWhat);

    one::clproto::ServerMessage serverMsg;
    serverMsg.set_message_id(clientMsg.message_id());
    auto status = serverMsg.mutable_status();
    status->set_code(one::clproto::Status_Code_ok);
    status->set_description(description);

    return serverMsg.SerializeAsString();
}

BOOST_PYTHON_MODULE(communication_stack)
{
    class_<CommunicatorProxy, boost::noncopyable>("Communicator", no_init)
        .def("__init__", make_constructor(create))
        .def("stop", &CommunicatorProxy::stop)
        .def("connect", &CommunicatorProxy::connect)
        .def("send", &CommunicatorProxy::send)
        .def("sendAsync", &CommunicatorProxy::sendAsync)
        .def("communicate", &CommunicatorProxy::communicate)
        .def("communicateReceive", &CommunicatorProxy::communicateReceive)
        .def("setHandshake", &CommunicatorProxy::setHandshake)
        .def("handshakeResponse", &CommunicatorProxy::handshakeResponse);

    def("prepareReply", prepareReply);
}
