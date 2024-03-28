/**
 * @file faInStorageHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "posixHelper.h"
#include "storageFanInHelper.h"

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

namespace {
struct Stat {
    time_t atime;
    time_t mtime;
    time_t ctime;
    int gid;
    int uid;
    int mode;
    size_t size;

    static Stat fromStat(const struct stat &attr)
    {
        Stat res;
        res.size = attr.st_size;
        res.atime = attr.st_atim.tv_sec;
        res.mtime = attr.st_mtim.tv_sec;
        res.ctime = attr.st_ctim.tv_sec;
        res.gid = attr.st_gid;
        res.uid = attr.st_uid;
        res.mode = attr.st_mode;

        return res;
    }

    bool operator==(const Stat &o) const
    {
        return atime == o.atime && mtime == o.mtime && ctime == o.ctime &&
            gid == o.gid && uid == o.uid && mode == o.mode && size == o.size;
    }
};
}

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class FanInStorageHelperProxy {
public:
    /**
     * Test FanInStorage proxy based on an FanInStorage Helper
     */
    FanInStorageHelperProxy(std::string mountPoint, uid_t uid, gid_t gid)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              POSIX_HELPER_WORKER_THREADS,
              std::make_shared<StorageWorkerFactory>("posix_t"))}
    {
        Params params;
        params["type"] = "posix";
        params["uid"] = std::to_string(uid);
        params["gid"] = std::to_string(gid);

        std::vector<std::string> mountPoints;
        folly::split(":", mountPoint, mountPoints, true);

        std::vector<StorageHelperPtr> storages;

        auto factory = PosixHelperFactory{m_executor};

        for (const auto &mp : mountPoints) {
            Params argsCopy{params};
            argsCopy["mountPoint"] = mp;
            storages.emplace_back(factory.createStorageHelper(
                argsCopy, ExecutionContext::ONECLIENT));
        }

        m_helper = std::make_shared<StorageFanInHelper>(
            std::move(storages), m_executor, ExecutionContext::ONECLIENT);
    }

    ~FanInStorageHelperProxy() { m_executor->join(); }

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
                    .thenValue([handle](auto &&buf) {
                        std::string data;
                        std::move(buf).appendToString(data);
                        return data;
                    });
            })
            .get();
    }

    Stat getattr(std::string fileId)
    {
        ReleaseGIL guard;
        auto attr = m_helper->getattr(fileId).get();

        return Stat::fromStat(attr);
    }

    void access(std::string fileId)
    {
        ReleaseGIL guard;
        m_helper->access(fileId, {}).get();
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

    std::string getxattr(std::string fileId, std::string name)
    {
        ReleaseGIL guard;
        return m_helper->getxattr(fileId, name).get().toStdString();
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

    size_t blockSize()
    {
        ReleaseGIL guard;
        return m_helper->blockSize();
    }

    std::string storagePathType()
    {
        ReleaseGIL guard;
        return m_helper->storagePathType() == StoragePathType::FLAT
            ? "flat"
            : "canonical";
    }

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    std::shared_ptr<one::helpers::StorageFanInHelper> m_helper;
};

namespace {
boost::shared_ptr<FanInStorageHelperProxy> create(
    std::string mountpoint, int uid, int gid)
{
    return boost::make_shared<FanInStorageHelperProxy>(mountpoint, uid, gid);
}
} // namespace

BOOST_PYTHON_MODULE(faninstorage_helper)
{
    class_<Stat>("Stat")
        .def_readwrite("st_atime", &Stat::atime)
        .def_readwrite("st_mtime", &Stat::mtime)
        .def_readwrite("st_ctime", &Stat::ctime)
        .def_readwrite("st_gid", &Stat::gid)
        .def_readwrite("st_uid", &Stat::uid)
        .def_readwrite("st_mode", &Stat::mode)
        .def_readwrite("st_size", &Stat::size)
        .def("__eq__", &Stat::operator==);

    class_<FanInStorageHelperProxy, boost::noncopyable>(
        "FanInStorageHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("access", &FanInStorageHelperProxy::access)
        .def("getattr", &FanInStorageHelperProxy::getattr)
        .def("readdir", &FanInStorageHelperProxy::readdir)
        .def("read", &FanInStorageHelperProxy::read)
        .def("readlink", &FanInStorageHelperProxy::readlink)
        .def("blockSize", &FanInStorageHelperProxy::blockSize)
        .def("storagePathType", &FanInStorageHelperProxy::storagePathType);
}
