/**
 * @file proxyIOProxy.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "proxyHelper.h"

#include "buffering/bufferAgent.h"
#include "communication/communicator.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>

#include <string>

using namespace boost::python;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class BufferAgentProxy {
public:
    BufferAgentProxy(
        std::string storageId, std::string host, const unsigned short port)
        : m_communicator{1, 1, host, port, false, true, false}
        , m_scheduler{std::make_shared<one::Scheduler>(1)}
        , m_helper{std::make_shared<one::helpers::buffering::BufferAgent>(
              one::helpers::buffering::BufferLimits{},
              std::make_shared<
                  one::helpers::ProxyHelper<one::communication::Communicator>>(
                  storageId, m_communicator),
              m_scheduler,
              std::make_shared<
                  one::helpers::buffering::BufferAgentsMemoryLimitGuard>(
                  one::helpers::buffering::BufferLimits{}))}
    {
        m_communicator.setScheduler(m_scheduler);
        m_communicator.connect();
    }

    ~BufferAgentProxy() { stop(); }

    void stop()
    {
        ReleaseGIL guard;
        if (!m_stopped.test_and_set()) {
            m_communicator.stop();
        }
    }

    bool isBuffered() { return m_helper->isBuffered(); }

    one::helpers::FileHandlePtr open(std::string fileId,
        const std::unordered_map<folly::fbstring, folly::fbstring> &parameters)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, parameters).get();
    }

    int write(one::helpers::FileHandlePtr handle, std::string data, int offset)
    {
        ReleaseGIL guard;
        folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
        buf.append(std::move(data));
        return handle->write(offset, std::move(buf), {})
            .thenValue([handle](int &&size) { return size; })
            .get();
    }

    std::string read(one::helpers::FileHandlePtr handle, int offset, int size)
    {
        ReleaseGIL guard;
        return handle->read(offset, size)
            .thenTry([handle](folly::Try<folly::IOBufQueue> buf) {
                std::string data;
                std::move(buf.value()).appendToString(data);
                return data;
            })
            .get();
    }

    void release(one::helpers::FileHandlePtr handle)
    {
        ReleaseGIL guard;

        assert(m_communicator.executor().get() ==
            handle->helper()->executor().get());

        handle->release().get();
    }

private:
    one::communication::Communicator m_communicator;
    std::shared_ptr<one::Scheduler> m_scheduler;
    std::shared_ptr<one::helpers::buffering::BufferAgent> m_helper;
    std::atomic_flag m_stopped = ATOMIC_FLAG_INIT;
};

namespace {
boost::shared_ptr<BufferAgentProxy> create(
    std::string storageId, std::string host, int port)
{
    return boost::make_shared<BufferAgentProxy>(storageId, host, port);
}
}

one::helpers::FileHandlePtr raw_open(tuple args, dict kwargs)
{
    std::string fileId = extract<std::string>(args[1]);
    dict parametersDict = extract<dict>(args[2]);

    std::unordered_map<folly::fbstring, folly::fbstring> parametersMap;
    list keys = parametersDict.keys();
    for (int i = 0; i < len(keys); ++i) {
        std::string key = extract<std::string>(keys[i]);
        std::string val = extract<std::string>(parametersDict[key]);
        parametersMap[key] = val;
    }

    return extract<BufferAgentProxy &>(args[0])().open(fileId, parametersMap);
}

BOOST_PYTHON_MODULE(buffer_agent)
{
    class_<one::helpers::FileHandlePtr>("FileHandle", no_init);

    class_<BufferAgentProxy, boost::noncopyable>("BufferAgentProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("open", raw_function(raw_open))
        .def("is_buffered", &BufferAgentProxy::isBuffered)
        .def("stop", &BufferAgentProxy::stop)
        .def("read", &BufferAgentProxy::read)
        .def("write", &BufferAgentProxy::write)
        .def("release", &BufferAgentProxy::release);
}
