/**
 * @file connectionPoolProxy.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "communication/connectionPool.h"
#include "helpers/init.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <glog/logging.h>
#include <tbb/concurrent_queue.h>

#include <atomic>
#include <string>

using namespace boost::python;
using namespace one::communication;

class ConnectionPoolProxy {
public:
    ConnectionPoolProxy(const std::size_t conn, const std::size_t workers,
        std::string host, const unsigned short port)
        : m_pool{conn, workers, std::move(host), port, false, false, false}
    {
        m_pool.setOnMessageCallback([this](std::string msg) {
            m_messages.emplace(std::move(msg));
            ++m_size;
        });

        m_pool.connect();
    }

    ~ConnectionPoolProxy() { stop(); }

    void stop()
    {
        if (!m_stopped.test_and_set()) {
            m_pool.stop();
        }
    }

    void send(const std::string &msg)
    {
        m_pool
            .send(
                msg, [](auto) {}, int{})
            .get();
    }

    size_t sentMessageCounter() { return m_pool.sentMessageCounter(); }

    size_t queuedMessageCounter() { return m_pool.queuedMessageCounter(); }

    std::string popMessage()
    {
        std::string msg;
        m_messages.try_pop(msg);
        return msg;
    }

    size_t size() { return m_size; }

private:
    ConnectionPool m_pool;
    std::atomic<std::size_t> m_size{0};
    tbb::concurrent_queue<std::string> m_messages;
    std::atomic_flag m_stopped = ATOMIC_FLAG_INIT;
};

namespace {
boost::shared_ptr<ConnectionPoolProxy> create(
    int conn, int workers, std::string host, int port)
{
    FLAGS_v = 0;

    one::helpers::init();

    return boost::make_shared<ConnectionPoolProxy>(
        conn, workers, std::move(host), port);
}
}

BOOST_PYTHON_MODULE(connection_pool)
{
    class_<ConnectionPoolProxy, boost::noncopyable>(
        "ConnectionPoolProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("stop", &ConnectionPoolProxy::stop)
        .def("send", &ConnectionPoolProxy::send)
        .def("popMessage", &ConnectionPoolProxy::popMessage)
        .def("size", &ConnectionPoolProxy::size)
        .def("sentMessageCounter", &ConnectionPoolProxy::sentMessageCounter)
        .def(
            "queuedMessageCounter", &ConnectionPoolProxy::queuedMessageCounter);
}
