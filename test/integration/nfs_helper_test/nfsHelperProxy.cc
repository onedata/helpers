/**
 * @file nfsHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nfsHelper.h"

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
 * Minimum 2 threads are required to run helper.
 */
constexpr int NFS_HELPER_WORKER_THREADS = 10;

constexpr std::chrono::milliseconds NFS_HELPER_PROXY_TIMEOUT_MS{10 * 1000};

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class NFSHelperProxy {
public:
    NFSHelperProxy(
        std::string host, std::string volume, uid_t uid, gid_t gid, int version)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              NFS_HELPER_WORKER_THREADS,
              std::make_shared<StorageWorkerFactory>("nfs_t"))}
    {
        one::helpers::Params params;
        params["type"] = "nfs";
        params["host"] = host;
        params["volume"] = volume;
        params["uid"] = std::to_string(uid);
        params["gid"] = std::to_string(gid);
        params["version"] = std::to_string(version);
        params["timeout"] = std::to_string(NFS_HELPER_PROXY_TIMEOUT_MS.count());

        m_helper = std::make_shared<one::helpers::NFSHelper>(
            NFSHelperParams::create(params), m_executor,
            ExecutionContext::ONECLIENT);
    }

    void checkStorageAvailability()
    {
        ReleaseGIL guard;
        m_helper->checkStorageAvailability().get();
    }

    void open(std::string fileId, int flags)
    {
        ReleaseGIL guard;
        m_helper->open(fileId, flags, {})
            .thenValue(
                [&, helper = m_helper](one::helpers::FileHandlePtr &&handle) {
                    handle->release();
                });
    }

    auto read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, O_RDONLY, {})
            .thenValue([&, helper = m_helper](
                           one::helpers::FileHandlePtr &&handle) {
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
        return m_helper->open(fileId, O_RDWR | O_CREAT, {})
            .thenValue([&, helper = m_helper](
                           one::helpers::FileHandlePtr &&handle) {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(data);
                return handle->write(offset, std::move(buf), {})
                    .thenValue([handle](auto &&size) { return size; });
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

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::NFSHelper> m_helper;
};

namespace {
boost::shared_ptr<NFSHelperProxy> create(
    std::string host, std::string volume, uid_t uid, gid_t gid, int version)
{
    FLAGS_v = 0;

    return boost::make_shared<NFSHelperProxy>(host, volume, uid, gid, version);
}
} // namespace

BOOST_PYTHON_MODULE(nfs_helper)
{
    class_<NFSHelperProxy, boost::noncopyable>("NFSHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("open", &NFSHelperProxy::open)
        .def("read", &NFSHelperProxy::read)
        .def("write", &NFSHelperProxy::write)
        .def("getattr", &NFSHelperProxy::getattr)
        .def("readdir", &NFSHelperProxy::readdir)
        .def("readlink", &NFSHelperProxy::readlink)
        .def("mknod", &NFSHelperProxy::mknod)
        .def("mkdir", &NFSHelperProxy::mkdir)
        .def("unlink", &NFSHelperProxy::unlink)
        .def("rmdir", &NFSHelperProxy::rmdir)
        .def("symlink", &NFSHelperProxy::symlink)
        .def("rename", &NFSHelperProxy::rename)
        .def("link", &NFSHelperProxy::link)
        .def("chmod", &NFSHelperProxy::chmod)
        .def("chown", &NFSHelperProxy::chown)
        .def("truncate", &NFSHelperProxy::truncate)
        .def("check_storage_availability",
            &NFSHelperProxy::checkStorageAvailability);
}
