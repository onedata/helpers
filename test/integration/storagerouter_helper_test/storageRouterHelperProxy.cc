/**
 * @file storageRouterHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "posixHelper.h"
#include "storageRouterHelper.h"

#include <asio/buffer.hpp>
#include <asio/io_service.hpp>
#include <asio/ts/executor.hpp>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include <iostream>

using ReadDirResult = std::vector<std::string>;

using namespace boost::python;
using namespace one::helpers;

constexpr auto POSIX_HELPER_WORKER_THREADS = 4;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class StorageRouterHelperProxy {
public:
    /**
     * Test StorageRouter on 2 Posix helpers
     */
    StorageRouterHelperProxy(std::string routeA, std::string mountPointA,
        std::string routeB, std::string mountPointB, uid_t uid, gid_t gid)
        : m_service{POSIX_HELPER_WORKER_THREADS}
        , m_idleWork{asio::make_work_guard(m_service)}
    {
        std::unordered_map<folly::fbstring, folly::fbstring> params;
        params["type"] = "posix";
        params["mountPoint"] = mountPointA;
        params["uid"] = std::to_string(uid);
        params["gid"] = std::to_string(gid);

        auto helperA = std::make_shared<one::helpers::PosixHelper>(
            PosixHelperParams::create(params),
            std::make_shared<one::AsioExecutor>(m_service),
            ExecutionContext::ONECLIENT);

        params["mountPoint"] = mountPointB;

        auto helperB = std::make_shared<one::helpers::PosixHelper>(
            PosixHelperParams::create(params),
            std::make_shared<one::AsioExecutor>(m_service),
            ExecutionContext::ONECLIENT);

        std::map<folly::fbstring, StorageHelperPtr> helperMap;
        helperMap.emplace(routeA, std::move(helperA));
        helperMap.emplace(routeB, std::move(helperB));

        m_helper = std::make_shared<one::helpers::StorageRouterHelper>(
            std::move(helperMap), ExecutionContext::ONECLIENT);

        for (int i = 0; i < POSIX_HELPER_WORKER_THREADS; i++) {
            m_workers.push_back(std::thread([=]() { m_service.run(); }));
        }
    }

    ~StorageRouterHelperProxy()
    {
        m_service.stop();
        for (auto &worker : m_workers) {
            worker.join();
        }
    }

    void open(std::string fileId, int flags)
    {
        ReleaseGIL guard;
        m_helper->open(fileId, flags, {})
            .then(
                [&](one::helpers::FileHandlePtr handle) { handle->release(); });
    }

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, O_RDONLY, {})
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

        // PosixHelper does not support creating file with approriate mode,
        // so in case it doesn't exist we have to create it first to be
        // compatible with other helpers' test cases.
        auto mknodLambda = [&] {
            return m_helper->mknod(fileId, S_IFREG | 0666, {}, 0).get();
        };
        auto writeLambda = [&] {
            return m_helper->open(fileId, O_WRONLY, {})
                .then([&](one::helpers::FileHandlePtr handle) {
                    folly::IOBufQueue buf{
                        folly::IOBufQueue::cacheChainLength()};
                    buf.append(data);
                    return handle->write(offset, std::move(buf), {}).get();
                })
                .get();
        };

        return m_helper->access(fileId, 0)
            .then(writeLambda)
            .onError([mknodLambda = std::move(mknodLambda),
                         writeLambda = std::move(writeLambda),
                         executor = std::make_shared<one::AsioExecutor>(
                             m_service)](std::exception const &e) {
                return folly::via(executor.get(), mknodLambda)
                    .then(writeLambda)
                    .get();
            })
            .get();
    }

    struct stat getattr(std::string fileId)
    {
        ReleaseGIL guard;
        return m_helper->getattr(fileId).get();
    }

    void access(std::string fileId, int mask)
    {
        ReleaseGIL guard;
        m_helper->access(fileId, mask).get();
    }

    ReadDirResult readdir(std::string fileId, int offset, int count)
    {
        ReleaseGIL guard;
        std::vector<std::string> res;
        for (auto &direntry : m_helper->readdir(fileId, offset, count).get()) {
            res.emplace_back(direntry.toStdString());
        }
        return res;
    }

    std::string readlink(std::string fileId)
    {
        ReleaseGIL guard;
        return m_helper->readlink(fileId).get().toStdString();
    }

    void mknod(std::string fileId, mode_t mode, std::vector<Flag> flags)
    {
        ReleaseGIL guard;
        m_helper->mknod(fileId, mode, FlagsSet(flags.begin(), flags.end()), 0)
            .get();
    }

    void mkdir(std::string fileId, mode_t mode)
    {
        ReleaseGIL guard;
        m_helper->mkdir(fileId, mode).get();
    }

    void unlink(std::string fileId, int size)
    {
        ReleaseGIL guard;
        m_helper->unlink(fileId, size).get();
    }

    void rmdir(std::string fileId)
    {
        ReleaseGIL guard;
        m_helper->rmdir(fileId).get();
    }

    void symlink(std::string from, std::string to)
    {
        ReleaseGIL guard;
        m_helper->symlink(from, to).get();
    }

    void rename(std::string from, std::string to)
    {
        ReleaseGIL guard;
        m_helper->rename(from, to).get();
    }

    void link(std::string from, std::string to)
    {
        ReleaseGIL guard;
        m_helper->link(from, to).get();
    }

    void chmod(std::string fileId, mode_t mode)
    {
        ReleaseGIL guard;
        m_helper->chmod(fileId, mode).get();
    }

    void chown(std::string fileId, uid_t uid, gid_t gid)
    {
        ReleaseGIL guard;
        m_helper->chown(fileId, uid, gid).get();
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

    std::string mountpoint(std::string route)
    {
        auto posixHelperPtr =
            std::dynamic_pointer_cast<PosixHelper>(m_helper->route(route));
        return posixHelperPtr->mountPoint().c_str();
    }

private:
    asio::io_service m_service;
    asio::executor_work_guard<asio::io_service::executor_type> m_idleWork;
    std::vector<std::thread> m_workers;
    std::shared_ptr<one::helpers::StorageRouterHelper> m_helper;
};

namespace {
boost::shared_ptr<StorageRouterHelperProxy> create(std::string routeA,
    std::string mountPointA, std::string routeB, std::string mountPointB,
    uid_t uid, gid_t gid)
{
    return boost::make_shared<StorageRouterHelperProxy>(std::move(routeA),
        std::move(mountPointA), std::move(routeB), std::move(mountPointB), uid,
        gid);
}
} // namespace

BOOST_PYTHON_MODULE(storagerouter_helper)
{
    class_<StorageRouterHelperProxy, boost::noncopyable>(
        "StorageRouterHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("open", &StorageRouterHelperProxy::open)
        .def("read", &StorageRouterHelperProxy::read)
        .def("write", &StorageRouterHelperProxy::write)
        .def("getattr", &StorageRouterHelperProxy::getattr)
        .def("readdir", &StorageRouterHelperProxy::readdir)
        .def("readlink", &StorageRouterHelperProxy::readlink)
        .def("mknod", &StorageRouterHelperProxy::mknod)
        .def("mkdir", &StorageRouterHelperProxy::mkdir)
        .def("unlink", &StorageRouterHelperProxy::unlink)
        .def("rmdir", &StorageRouterHelperProxy::rmdir)
        .def("symlink", &StorageRouterHelperProxy::symlink)
        .def("rename", &StorageRouterHelperProxy::rename)
        .def("link", &StorageRouterHelperProxy::link)
        .def("chmod", &StorageRouterHelperProxy::chmod)
        .def("chown", &StorageRouterHelperProxy::chown)
        .def("truncate", &StorageRouterHelperProxy::truncate)
        .def("getxattr", &StorageRouterHelperProxy::getxattr)
        .def("setxattr", &StorageRouterHelperProxy::setxattr)
        .def("removexattr", &StorageRouterHelperProxy::removexattr)
        .def("listxattr", &StorageRouterHelperProxy::listxattr)
        .def("mountpoint", &StorageRouterHelperProxy::mountpoint);
}