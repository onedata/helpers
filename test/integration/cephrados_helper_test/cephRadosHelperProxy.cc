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
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/system/ThreadName.h>

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
        std::size_t blockSize, std::string storagePathType)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              threadNumber, std::make_shared<StorageWorkerFactory>("rados_t"))}

    {
        using namespace one::helpers;

        Params params;
        params["clusterName"] = "ceph";
        params["monitorHostname"] = monHost;
        params["poolName"] = poolName;
        params["username"] = username;
        params["key"] = key;
        params["timeout"] = "20";
        params["blockSize"] = std::to_string(blockSize);
        params["storagePathType"] = storagePathType;

        auto parameters = CephRadosHelperParams::create(params);

        m_helper = std::make_shared<KeyValueAdapter>(
            std::make_shared<CephRadosHelper>(parameters), parameters,
            m_executor);
    }

    ~CephRadosHelperProxy() { }

    void unlink(std::string fileId, int size)
    {
        ReleaseGIL guard;
        m_helper->unlink(fileId, size).get();
    }

    auto read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                return handle->read(offset, size)
                    .thenValue([handle](folly::IOBufQueue &&buf) {
                        std::string data;
                        buf.appendToString(data);
                        return boost::python::api::object(
                            boost::python::handle<>(PyBytes_FromStringAndSize(
                                data.c_str(), data.size())));
                    });
            })
            .get();
    }

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(data);
                return handle->write(offset, std::move(buf), {})
                    .thenValue([handle](int &&size) { return size; });
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
    FLAGS_v = 0;

    return boost::make_shared<CephRadosHelperProxy>(std::move(monHost),
        std::move(username), std::move(key), std::move(poolName), threadNumber,
        blockSize, storagePathType);
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
