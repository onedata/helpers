/**
 * @file nullDeviceHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nullDeviceHelper.h"

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
constexpr int NULL_DEVICE_HELPER_WORKER_THREADS = 4;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

class NullDeviceHelperProxy {
public:
    NullDeviceHelperProxy(const int latencyMin, const int latencyMax,
        const double timeoutProbability, std::string filter,
        std::string simulatedFilesystemParameters,
        float simulatedFilesystemGrowSpeed, bool enableDataVerification)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              NULL_DEVICE_HELPER_WORKER_THREADS,
              std::make_shared<StorageWorkerFactory>("null_t"))}
    {
        auto simulatedFilesystemParams =
            NullDeviceHelperFactory::parseSimulatedFilesystemParameters(
                simulatedFilesystemParameters);

        m_helper = std::make_shared<one::helpers::NullDeviceHelper>(latencyMin,
            latencyMax, timeoutProbability, std::move(filter),
            std::get<0>(simulatedFilesystemParams),
            simulatedFilesystemGrowSpeed,
            std::get<1>(simulatedFilesystemParams)
                .value_or(NULL_DEVICE_DEFAULT_SIMULATED_FILE_SIZE),
            enableDataVerification, m_executor);
    }

    ~NullDeviceHelperProxy() { }

    void open(std::string fileId, int flags)
    {
        ReleaseGIL guard;
        m_helper->open(fileId, flags, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
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

        // NullDeviceHelper does not support creating file with approriate mode,
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
                [mknodLambda = mknodLambda, writeLambda = writeLambda,
                    executor = m_executor, helper = m_helper](auto &&e) {
                    return folly::makeSemiFuture()
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

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::NullDeviceHelper> m_helper;
};

namespace {
boost::shared_ptr<NullDeviceHelperProxy> create(const int latencyMin,
    const int latencyMax, const double timeoutProbability, std::string filter,
    std::string simulatedFilesystemParameters = "",
    float simulatedFilesystemGrowSpeed = 0.0,
    bool enableDataVerification = false)
{
    return boost::make_shared<NullDeviceHelperProxy>(latencyMin, latencyMax,
        timeoutProbability, std::move(filter),
        std::move(simulatedFilesystemParameters), simulatedFilesystemGrowSpeed,
        enableDataVerification);
}
} // namespace

BOOST_PYTHON_MODULE(nulldevice_helper)
{
    class_<NullDeviceHelperProxy, boost::noncopyable>(
        "NullDeviceHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("open", &NullDeviceHelperProxy::open)
        .def("read", &NullDeviceHelperProxy::read)
        .def("write", &NullDeviceHelperProxy::write)
        .def("getattr", &NullDeviceHelperProxy::getattr)
        .def("readdir", &NullDeviceHelperProxy::readdir)
        .def("readlink", &NullDeviceHelperProxy::readlink)
        .def("mknod", &NullDeviceHelperProxy::mknod)
        .def("mkdir", &NullDeviceHelperProxy::mkdir)
        .def("unlink", &NullDeviceHelperProxy::unlink)
        .def("rmdir", &NullDeviceHelperProxy::rmdir)
        .def("symlink", &NullDeviceHelperProxy::symlink)
        .def("rename", &NullDeviceHelperProxy::rename)
        .def("link", &NullDeviceHelperProxy::link)
        .def("chmod", &NullDeviceHelperProxy::chmod)
        .def("chown", &NullDeviceHelperProxy::chown)
        .def("truncate", &NullDeviceHelperProxy::truncate)
        .def("getxattr", &NullDeviceHelperProxy::getxattr)
        .def("setxattr", &NullDeviceHelperProxy::setxattr)
        .def("removexattr", &NullDeviceHelperProxy::removexattr)
        .def("listxattr", &NullDeviceHelperProxy::listxattr);
}
