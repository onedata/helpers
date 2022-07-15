/**
 * @file posixHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "posixHelper.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include <iostream>

using ReadDirResult = std::vector<std::string>;

using namespace boost::python;
using namespace one::helpers;

/*
 * Minimum 4 threads are required to run this helper proxy.
 */
constexpr int POSIX_HELPER_WORKER_THREADS = 1;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class PosixHelperProxy {
public:
    PosixHelperProxy(std::string mountPoint, uid_t uid, gid_t gid)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              POSIX_HELPER_WORKER_THREADS,
              std::make_shared<StorageWorkerFactory>("posix_t"))}
    {
        std::unordered_map<folly::fbstring, folly::fbstring> params;
        params["type"] = "posix";
        params["mountPoint"] = mountPoint;
        params["uid"] = std::to_string(uid);
        params["gid"] = std::to_string(gid);

        m_helper = std::make_shared<one::helpers::PosixHelper>(
            PosixHelperParams::create(params), m_executor,
            ExecutionContext::ONECLIENT);
    }

    ~PosixHelperProxy() { m_executor->join(); }

    void open(std::string fileId, int flags)
    {
        ReleaseGIL guard;
        m_helper->open(fileId, flags, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                handle->release();
            });
    }

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, O_RDONLY, {})
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

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;

        // PosixHelper does not support creating file with approriate mode,
        // so in case it doesn't exist we have to create it first to be
        // compatible with other helpers' test cases.
        auto mknodLambda = [&](auto && /*unit*/) {
            return m_helper->mknod(fileId, S_IFREG | 0666, {}, 0);
        };
        auto writeLambda = [&](auto && /*unit*/) {
            return m_helper->open(fileId, O_WRONLY, {})
                .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                    folly::IOBufQueue buf{
                        folly::IOBufQueue::cacheChainLength()};
                    buf.append(data);
                    return handle->write(offset, std::move(buf), {})
                        .thenValue([handle](auto &&size) { return size; });
                });
        };

        return m_helper->access(fileId, 0)
            .thenValue(writeLambda)
            .thenError(folly::tag_t<std::exception>{},
                [mknodLambda = std::move(mknodLambda),
                    writeLambda = std::move(writeLambda),
                    executor = m_executor](auto &&e) {
                    return folly::makeFuture()
                        .via(executor.get())
                        .thenValue(mknodLambda)
                        .thenValue(writeLambda);
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

    void refreshParams(std::string mountPoint, int uid, int gid)
    {
        std::unordered_map<folly::fbstring, folly::fbstring> params;
        params["type"] = "posix";
        params["mountPoint"] = mountPoint;
        params["uid"] = std::to_string(uid);
        params["gid"] = std::to_string(gid);

        auto p = PosixHelperParams::create(params);

        m_helper->refreshParams(std::move(p)).get();
    }

    std::string mountpoint() { return m_helper->mountPoint().c_str(); }

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    std::shared_ptr<one::helpers::PosixHelper> m_helper;
};

/*namespace {*/
//boost::shared_ptr<PosixHelperProxy> create(
//    std::string mountPoint, uid_t uid, gid_t gid)
//{
//    return boost::make_shared<PosixHelperProxy>(
//        std::move(mountPoint), uid, gid);
//}
//} // namespace
/**/
BOOST_PYTHON_MODULE(posix_helper)
{
    //Py_Initialize();
    class_<PosixHelperProxy, boost::noncopyable>("PosixHelperProxy", init<std::string, uid_t, gid_t>())
        //.def("__init__", make_constructor(create))
        .def("open", &PosixHelperProxy::open)
        .def("read", &PosixHelperProxy::read)
        .def("write", &PosixHelperProxy::write)
        .def("getattr", &PosixHelperProxy::getattr)
        .def("readdir", &PosixHelperProxy::readdir)
        .def("readlink", &PosixHelperProxy::readlink)
        .def("mknod", &PosixHelperProxy::mknod)
        .def("mkdir", &PosixHelperProxy::mkdir)
        .def("unlink", &PosixHelperProxy::unlink)
        .def("rmdir", &PosixHelperProxy::rmdir)
        .def("symlink", &PosixHelperProxy::symlink)
        .def("rename", &PosixHelperProxy::rename)
        .def("link", &PosixHelperProxy::link)
        .def("chmod", &PosixHelperProxy::chmod)
        .def("chown", &PosixHelperProxy::chown)
        .def("truncate", &PosixHelperProxy::truncate)
        .def("getxattr", &PosixHelperProxy::getxattr)
        .def("setxattr", &PosixHelperProxy::setxattr)
        .def("removexattr", &PosixHelperProxy::removexattr)
        .def("listxattr", &PosixHelperProxy::listxattr)
        .def("refresh_params", &PosixHelperProxy::refreshParams)
        .def("mountpoint", &PosixHelperProxy::mountpoint);
}
