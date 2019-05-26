/**
 * @file cephRadosHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephRadosHelper.h"

#include <asio/buffer.hpp>
#include <asio/io_service.hpp>
#include <asio/ts/executor.hpp>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <folly/ThreadName.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

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

class CephRadosHelperProxy {
public:
    CephRadosHelperProxy(std::string monHost, std::string username,
        std::string key, std::string poolName, int threadNumber,
        std::size_t blockSize)
        : m_service{threadNumber}
        , m_idleWork{asio::make_work_guard(m_service)}
        , m_helper{std::make_shared<one::helpers::KeyValueAdapter>(
              std::make_shared<one::helpers::CephRadosHelper>(
                  "ceph", monHost, poolName, username, key),
              std::make_shared<one::AsioExecutor>(m_service), blockSize)}
    {
        std::generate_n(std::back_inserter(m_workers), threadNumber, [=] {
            std::thread t{[=] {
                folly::setThreadName("CRHProxy");
                m_service.run();
            }};

            return t;
        });
    }

    ~CephRadosHelperProxy()
    {
        m_service.stop();
        for (auto &t : m_workers)
            t.join();
    }

    void unlink(std::string fileId, int size)
    {
        ReleaseGIL guard;
        m_helper->unlink(fileId, size).get();
    }

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .then([&](one::helpers::FileHandlePtr handle) {
                auto buf = handle->read(offset, size).get();
                std::string data;
                buf.appendToString(data);
                return data;
            })
            .get();
    }

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .then([&](one::helpers::FileHandlePtr handle) {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(data);
                return handle->write(offset, std::move(buf)).get();
            })
            .get();
    }

    void truncate(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, offset, size).get();
    }

private:
    asio::io_service m_service;
    asio::executor_work_guard<asio::io_service::executor_type> m_idleWork;
    std::vector<std::thread> m_workers;
    std::shared_ptr<one::helpers::StorageHelper> m_helper;
};

namespace {
boost::shared_ptr<CephRadosHelperProxy> create(std::string monHost,
    std::string username, std::string key, std::string poolName,
    std::size_t threadNumber, std::size_t blockSize)
{
    return boost::make_shared<CephRadosHelperProxy>(std::move(monHost),
        std::move(username), std::move(key), std::move(poolName), threadNumber,
        blockSize);
}
} // namespace

BOOST_PYTHON_MODULE(cephrados_helper)
{
    class_<CephRadosHelperProxy, boost::noncopyable>(
        "CephRadosHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("unlink", &CephRadosHelperProxy::unlink)
        .def("read", &CephRadosHelperProxy::read)
        .def("write", &CephRadosHelperProxy::write)
        .def("truncate", &CephRadosHelperProxy::truncate);
}
