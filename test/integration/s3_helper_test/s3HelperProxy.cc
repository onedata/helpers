/**
 * @file s3HelperProxy.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "communication/communicator.h"
#include "helpers/storageHelperCreator.h"
#include "keyValueAdapter.h"
#include "posixHelper.h"
#include "s3Helper.h"

#include <aws/s3/S3Client.h>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/system/ThreadName.h>

#include <algorithm>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace boost::python;
using namespace one::helpers;

namespace {
template <typename T1, typename T2> struct PairToPythonConverter {
    static PyObject *convert(const std::pair<T1, T2> &pair)
    {
        return incref(boost::python::make_tuple(pair.first, pair.second).ptr());
    }
};

template <typename T1, typename T2> struct py_pair {
    to_python_converter<std::pair<T1, T2>, PairToPythonConverter<T1, T2>> toPy;
};

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

using ReadDirResult = std::pair<std::string, std::vector<std::string>>;

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
        int threadNumber, std::size_t blockSize, std::string storagePathType,
        int timeout)
        : m_communicator{1, 1, "", 8080, false, true, false}
        , m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              threadNumber, std::make_shared<StorageWorkerFactory>("s3_t"))}
        , m_helperFactory{m_executor, m_executor, m_executor, m_executor,
              m_executor, m_executor, m_executor, m_executor, m_executor,
              m_executor, m_communicator}
    {
        using namespace one::helpers;

        Params params;
        params["type"] = "s3";
        params["name"] = "someS3";
        params["scheme"] = scheme;
        params["hostname"] = hostName;
        params["bucketName"] = bucketName;
        params["accessKey"] = accessKey;
        params["secretKey"] = secretKey;
        params["timeout"] = std::to_string(timeout * 1000);
        params["blockSize"] = std::to_string(blockSize);
        params["storagePathType"] = storagePathType;
        params["maxCanonicalObjectSize"] =
            std::to_string(kMaxCanonicakObjectSize);
        params["maxConnections"] = "25";

        auto parameters = S3HelperParams::create(params);

        m_helper = m_helperFactory.getStorageHelper(params, false);
    }

    ~S3HelperProxy() { }

    void checkStorageAvailability()
    {
        ReleaseGIL guard;
        m_helper->checkStorageAvailability().get();
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
        auto listobjectsResult =
            m_helper->listobjects(fileId, marker, 0, count).get();
        res.first = listobjectsResult.first.toStdString();

        for (auto &direntry : std::get<1>(listobjectsResult)) {
            res.second.emplace_back(std::get<0>(direntry).toStdString());
        }

        return res;
    }

    void multipartCopy(std::string source, std::string destination, int size)
    {
        ReleaseGIL guard;
        m_helper->multipartCopy(
                    source, destination, m_helper->blockSize(), size)
            .get();
    }

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
                return handle->read(offset, size);
            })
            .thenValue([](auto &&ioBuf) {
                std::string data;
                ioBuf.appendToString(data);
                return boost::python::api::object(boost::python::handle<>(
                    PyBytes_FromStringAndSize(data.c_str(), data.size())));
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
                return handle->write(offset, std::move(buf), {});
            })
            .get();
    }

    void truncate(std::string fileId, int size, int currentSize)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, size, currentSize).get();
    }

    void updateHelper(std::string scheme, std::string hostName,
        std::string bucketName, std::string accessKey, std::string secretKey,
        std::size_t threadNumber, std::size_t blockSize,
        std::string storagePathType)
    {
        Params params;
        params.emplace("type", "s3");
        params.emplace("scheme", scheme);
        params.emplace("hostname", hostName);
        params.emplace("bucketName", bucketName);
        params.emplace("accessKey", accessKey);
        params.emplace("secretKey", secretKey);
        params.emplace("threadNumber", std::to_string(threadNumber));
        params.emplace("blockSize", std::to_string(blockSize));
        params.emplace("storagePathType", storagePathType);

        m_helper->updateHelper(params).get();
    }

    bool isObjectStorage()
    {
        ReleaseGIL guard;
        return m_helper->isObjectStorage();
    }

    size_t blockSize()
    {
        ReleaseGIL guard;
        return m_helper->blockSize();
    }

    size_t blockSizeForPath(std::string path)
    {
        ReleaseGIL guard;
        return m_helper->blockSizeForPath(path).get();
    }

    std::string storagePathType()
    {
        ReleaseGIL guard;
        return m_helper->storagePathType() == StoragePathType::CANONICAL
            ? "canonical"
            : "flat";
    }

private:
    one::communication::Communicator m_communicator;
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    StorageHelperPtr m_helper;
    one::helpers::StorageHelperCreator<one::communication::Communicator>
        m_helperFactory;
};

namespace {
boost::shared_ptr<S3HelperProxy> create(std::string scheme,
    std::string hostName, std::string bucketName, std::string accessKey,
    std::string secretKey, std::size_t threadNumber, std::size_t blockSize,
    std::string storagePathType = "flat", int timeout = 20)
{
    FLAGS_v = 0;

    return boost::make_shared<S3HelperProxy>(std::move(scheme),
        std::move(hostName), std::move(bucketName), std::move(accessKey),
        std::move(secretKey), threadNumber, blockSize, storagePathType,
        timeout);
}
}

BOOST_PYTHON_MODULE(s3_helper)
{
    py_pair<std::string, std::vector<std::string>>();

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
        .def("multipart_copy", &S3HelperProxy::multipartCopy)
        .def("unlink", &S3HelperProxy::unlink)
        .def("read", &S3HelperProxy::read)
        .def("write", &S3HelperProxy::write)
        .def("truncate", &S3HelperProxy::truncate)
        .def("update_helper", &S3HelperProxy::updateHelper)
        .def("is_object_storage", &S3HelperProxy::isObjectStorage)
        .def("block_size", &S3HelperProxy::blockSize)
        .def("block_size_for_path", &S3HelperProxy::blockSizeForPath)
        .def("storage_path_type", &S3HelperProxy::storagePathType)
        .def("check_storage_availability",
            &S3HelperProxy::checkStorageAvailability);
    ;
}
