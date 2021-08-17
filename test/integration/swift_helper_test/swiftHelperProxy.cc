/**
 * @file swiftHelperProxy.cc
 * @author Michal Wrona
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "keyValueAdapter.h"
#include "swiftHelper.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/system/ThreadName.h>

#include <algorithm>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <vector>

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

class SwiftHelperProxy {
public:
    SwiftHelperProxy(std::string authUrl, std::string containerName,
        std::string tenantName, std::string userName, std::string password,
        int threadNumber, std::size_t blockSize,
        StoragePathType storagePathType)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              threadNumber, std::make_shared<StorageWorkerFactory>("swift_t"))}
        , m_helper{std::make_shared<one::helpers::KeyValueAdapter>(
              std::make_shared<one::helpers::SwiftHelper>(
                  std::move(containerName), authUrl, tenantName, userName,
                  password, std::chrono::seconds{20}, storagePathType),
              m_executor, blockSize)}
    {
    }

    ~SwiftHelperProxy() { }

    void unlink(std::string fileId, int size)
    {
        ReleaseGIL guard;
        m_helper->unlink(fileId, size).get();
    }

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                return handle->read(offset, size)
                    .thenValue([handle](folly::IOBufQueue &&buf) {
                        std::string data;
                        buf.appendToString(data);
                        return data;
                    });
            })
            .get();
    }

    std::size_t write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(data);
                return handle->write(offset, std::move(buf), {})
                    .thenValue([handle](auto &&size) { return size; });
            })
            .get();
    }

    void truncate(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, offset, size).get();
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::StorageHelper> m_helper;
};

namespace {
boost::shared_ptr<SwiftHelperProxy> create(std::string authUrl,
    std::string containerName, std::string tenantName, std::string userName,
    std::string password, std::size_t threadNumber, std::size_t blockSize,
    std::string storagePathType = "flat")
{
    return boost::make_shared<SwiftHelperProxy>(std::move(authUrl),
        std::move(containerName), std::move(tenantName), std::move(userName),
        std::move(password), threadNumber, blockSize,
        storagePathType == "canonical" ? StoragePathType::CANONICAL
                                       : StoragePathType::FLAT);
}
}

BOOST_PYTHON_MODULE(swift_helper)
{
    class_<SwiftHelperProxy, boost::noncopyable>("SwiftHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("unlink", &SwiftHelperProxy::unlink)
        .def("read", &SwiftHelperProxy::read)
        .def("write", &SwiftHelperProxy::write)
        .def("truncate", &SwiftHelperProxy::truncate);
}
