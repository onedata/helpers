/**
 * @file s3HelperProxy.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "keyValueAdapter.h"
#include "posixHelper.h"
#include "s3Helper.h"

#include <asio/buffer.hpp>
#include <asio/io_service.hpp>
#include <asio/ts/executor.hpp>
#include <aws/s3/S3Client.h>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <folly/ThreadName.h>

#include <algorithm>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace boost::python;
using namespace one::helpers;

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
using ReadDirResult = std::vector<std::string>;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

constexpr std::size_t kMaxCanonicakObjectSize = 2 * 1024 * 1024;

class S3HelperProxy {
public:
    S3HelperProxy(std::string scheme, std::string hostName,
        std::string bucketName, std::string accessKey, std::string secretKey,
        int threadNumber, std::size_t blockSize)
        : m_service{threadNumber}
        , m_idleWork{asio::make_work_guard(m_service)}
        , m_helper{std::make_shared<one::helpers::KeyValueAdapter>(
              std::make_shared<one::helpers::S3Helper>(std::move(hostName),
                  std::move(bucketName), std::move(accessKey),
                  std::move(secretKey), 2 * 1024 * 1024, 0664, 0775,
                  scheme == "https"),
              std::make_shared<one::AsioExecutor>(m_service), blockSize)}
    {
        std::generate_n(std::back_inserter(m_workers), threadNumber, [=] {
            std::thread t{[=] {
                folly::setThreadName("S3HelperProxy");
                m_service.run();
            }};

            return t;
        });
    }

    ~S3HelperProxy()
    {
        m_service.stop();
        for (auto &t : m_workers)
            t.join();
    }

    void access(std::string fileId)
    {
        ReleaseGIL guard;
        m_helper->access(fileId, {}).get();
    }

    Stat getattr(std::string fileId)
    {
        ReleaseGIL guard;
        auto attr = m_helper->getattr(fileId).get();

        return Stat::fromStat(attr);
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

    std::size_t write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .then([&](one::helpers::FileHandlePtr handle) {
                folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
                buf.append(data);
                return handle->write(offset, std::move(buf));
            })
            .get();
    }

    void truncate(std::string fileId, int size, int currentSize)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, size, currentSize).get();
    }

private:
    asio::io_service m_service;
    asio::executor_work_guard<asio::io_service::executor_type> m_idleWork;
    std::vector<std::thread> m_workers;
    std::shared_ptr<one::helpers::StorageHelper> m_helper;
};

namespace {
boost::shared_ptr<S3HelperProxy> create(std::string scheme,
    std::string hostName, std::string bucketName, std::string accessKey,
    std::string secretKey, std::size_t threadNumber, std::size_t blockSize)
{
    return boost::make_shared<S3HelperProxy>(std::move(scheme),
        std::move(hostName), std::move(bucketName), std::move(accessKey),
        std::move(secretKey), threadNumber, blockSize);
}
}

BOOST_PYTHON_MODULE(s3_helper)
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

    class_<S3HelperProxy, boost::noncopyable>("S3HelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("access", &S3HelperProxy::access)
        .def("getattr", &S3HelperProxy::getattr)
        .def("mknod", &S3HelperProxy::mknod)
        .def("mkdir", &S3HelperProxy::mkdir)
        .def("listobjects", &S3HelperProxy::listobjects)
        .def("unlink", &S3HelperProxy::unlink)
        .def("read", &S3HelperProxy::read)
        .def("write", &S3HelperProxy::write)
        .def("truncate", &S3HelperProxy::truncate);
}
