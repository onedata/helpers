/**
 * @file cephRadosHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephRadosHelper.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <folly/ThreadName.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

using namespace boost::python;
using one::helpers::StoragePathType;
using one::helpers::StorageWorkerFactory;

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
        std::size_t blockSize, StoragePathType storagePathType)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              threadNumber, std::make_shared<StorageWorkerFactory>("rados_t"))}
        , m_helper{std::make_shared<one::helpers::KeyValueAdapter>(
              std::make_shared<one::helpers::CephRadosHelper>("ceph", monHost,
                  poolName, username, key, std::chrono::seconds{20},
                  storagePathType),
              m_executor, blockSize)}
    {
    }

    ~CephRadosHelperProxy() {}

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
                return handle->read(offset, size)
                    .then([handle](folly::IOBufQueue buf) {
                        std::string data;
                        buf.appendToString(data);
                        return data;
                    });
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
                return handle->write(offset, std::move(buf), {})
                    .then([handle](int size) { return size; });
            })
            .get();
    }

    void truncate(std::string fileId, int size, int currentSize)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, size, currentSize).get();
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::StorageHelper> m_helper;
};

namespace {
boost::shared_ptr<CephRadosHelperProxy> create(std::string monHost,
    std::string username, std::string key, std::string poolName,
    std::size_t threadNumber, std::size_t blockSize,
    std::string storagePathType = "flat")
{
    return boost::make_shared<CephRadosHelperProxy>(std::move(monHost),
        std::move(username), std::move(key), std::move(poolName), threadNumber,
        blockSize,
        storagePathType == "canonical" ? StoragePathType::CANONICAL
                                       : StoragePathType::FLAT);
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
