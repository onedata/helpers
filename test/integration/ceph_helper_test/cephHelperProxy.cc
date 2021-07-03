/**
 * @file cephHelperProxy.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephHelper.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

using namespace boost::python;
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

class CephHelperProxy {
public:
    CephHelperProxy(std::string monHost, std::string username, std::string key,
        std::string poolName)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              8, std::make_shared<StorageWorkerFactory>("ceph"))}
        , m_helper{std::make_shared<one::helpers::CephHelper>(
              "ceph", monHost, poolName, username, key, m_executor)}
    {
    }

    ~CephHelperProxy() {}

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

    void truncate(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, offset, size).get();
    }

    std::string getxattr(std::string fileId, std::string name)
    {
        ReleaseGIL guard;
        return m_helper->getxattr(fileId, name).get().toStdString();
    }

    void setxattr(std::string fileId, std::string name, std::string value,
        bool create, bool replace)
    {
        ReleaseGIL guard;
        m_helper->setxattr(fileId, name, value, create, replace).get();
    }

    void removexattr(std::string fileId, std::string name)
    {
        ReleaseGIL guard;
        m_helper->removexattr(fileId, name).get();
    }

    std::vector<std::string> listxattr(std::string fileId)
    {
        ReleaseGIL guard;
        std::vector<std::string> res;
        for (auto &xattr : m_helper->listxattr(fileId).get()) {
            res.emplace_back(xattr.toStdString());
        }
        return res;
    }

    int lock(std::string fileId, std::string cookie, bool exclusive)
    {
        using namespace one::helpers;

        if (exclusive) {
            return m_helper->getIoCTX().lock_exclusive(
                fileId + CEPH_STRIPER_FIRST_OBJECT_SUFFIX,
                CEPH_STRIPER_LOCK_NAME, cookie, "", NULL, 0);
        }
        else {
            return m_helper->getIoCTX().lock_shared(
                fileId + CEPH_STRIPER_FIRST_OBJECT_SUFFIX,
                CEPH_STRIPER_LOCK_NAME, cookie, "", "Tag", NULL, 0);
        }
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::CephHelper> m_helper;
};

namespace {
boost::shared_ptr<CephHelperProxy> create(std::string monHost,
    std::string username, std::string key, std::string poolName)
{
    return boost::make_shared<CephHelperProxy>(std::move(monHost),
        std::move(username), std::move(key), std::move(poolName));
}
} // namespace

BOOST_PYTHON_MODULE(ceph_helper)
{
    class_<CephHelperProxy, boost::noncopyable>("CephHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("unlink", &CephHelperProxy::unlink)
        .def("read", &CephHelperProxy::read)
        .def("write", &CephHelperProxy::write)
        .def("truncate", &CephHelperProxy::truncate)
        .def("getxattr", &CephHelperProxy::getxattr)
        .def("setxattr", &CephHelperProxy::setxattr)
        .def("removexattr", &CephHelperProxy::removexattr)
        .def("listxattr", &CephHelperProxy::listxattr)
        .def("lock", &CephHelperProxy::lock);
}
