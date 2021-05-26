/**
 * @file storageRouterHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "bufferedStorageHelper.h"
#include "keyValueAdapter.h"
#include "s3Helper.h"

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

class BufferedStorageHelperProxy {
public:
    /**
     * Test BufferedStorage proxy based on an S3Helper and a single buckeet
     */
    BufferedStorageHelperProxy(std::string scheme, std::string hostName,
        std::string bucketName, std::string accessKey, std::string secretKey,
        int threadNumber, int blockSize)
        : m_service{threadNumber}
        , m_idleWork{asio::make_work_guard(m_service)}
    {
        auto bufferStorageHelper =
            std::make_shared<one::helpers::KeyValueAdapter>(
                std::make_shared<one::helpers::S3Helper>(hostName, bucketName,
                    accessKey, secretKey, std::numeric_limits<size_t>::max(),
                    0664, 0775, scheme == "https", std::chrono::seconds{20},
                    StoragePathType::FLAT),
                std::make_shared<one::AsioExecutor>(m_service), blockSize);

        auto mainStorageHelper =
            std::make_shared<one::helpers::KeyValueAdapter>(
                std::make_shared<one::helpers::S3Helper>(hostName, bucketName,
                    accessKey, secretKey, std::numeric_limits<size_t>::max(),
                    0664, 0775, scheme == "https", std::chrono::seconds{20},
                    StoragePathType::CANONICAL),
                std::make_shared<one::AsioExecutor>(m_service), 0);

        m_helper = std::make_shared<one::helpers::BufferedStorageHelper>(
            std::move(bufferStorageHelper), std::move(mainStorageHelper),
            ExecutionContext::ONECLIENT, ".__onedata__buffer");

        for (int i = 0; i < threadNumber; i++) {
            m_workers.push_back(std::thread([=]() { m_service.run(); }));
        }
    }

    ~BufferedStorageHelperProxy()
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

    std::size_t write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, O_WRONLY, {})
            .then([&](one::helpers::FileHandlePtr handle) {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(data);
                return handle->write(offset, std::move(buf), {}).get();
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

    ReadDirResult listobjects(std::string fileId, std::string marker, int count)
    {
        ReleaseGIL guard;
        ReadDirResult res;
        for (auto &direntry :
            m_helper->listobjects(fileId, marker, 0, count).get()) {
            res.emplace_back(std::get<0>(direntry).toStdString());
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

    void truncate(std::string fileId, int size, int currentSize)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, size, currentSize).get();
    }

    void flushBuffer(std::string fileId, int size)
    {
        ReleaseGIL guard;
        m_helper->flushBuffer(fileId, size).get();
    }

private:
    asio::io_service m_service;
    asio::executor_work_guard<asio::io_service::executor_type> m_idleWork;
    std::vector<std::thread> m_workers;
    std::shared_ptr<one::helpers::BufferedStorageHelper> m_helper;
};

namespace {
boost::shared_ptr<BufferedStorageHelperProxy> create(std::string scheme,
    std::string hostName, std::string bucketName, std::string accessKey,
    std::string secretKey, int threadNumber, int blockSize)
{
    return boost::make_shared<BufferedStorageHelperProxy>(scheme, hostName,
        bucketName, accessKey, secretKey, threadNumber, blockSize);
}
} // namespace

BOOST_PYTHON_MODULE(bufferedstorage_helper)
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

    class_<BufferedStorageHelperProxy, boost::noncopyable>(
        "BufferedStorageHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("access", &BufferedStorageHelperProxy::access)
        .def("getattr", &BufferedStorageHelperProxy::getattr)
        .def("mknod", &BufferedStorageHelperProxy::mknod)
        .def("mkdir", &BufferedStorageHelperProxy::mkdir)
        .def("listobjects", &BufferedStorageHelperProxy::listobjects)
        .def("unlink", &BufferedStorageHelperProxy::unlink)
        .def("read", &BufferedStorageHelperProxy::read)
        .def("write", &BufferedStorageHelperProxy::write)
        .def("flushBuffer", &BufferedStorageHelperProxy::flushBuffer)
        .def("truncate", &BufferedStorageHelperProxy::truncate);
}
