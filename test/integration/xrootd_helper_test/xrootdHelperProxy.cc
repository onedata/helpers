/**
 * @file xrootdHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "posixHelper.h"
#include "xrootdHelper.h"

#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <folly/executors/GlobalExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

using namespace boost::python;

using ReadDirResult = std::vector<std::string>;

using one::helpers::Flag;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

constexpr auto kXRootDHelperThreadCount = 5u;
constexpr auto kXRootDConnectionPoolSize = 10u;
constexpr auto kXRootDMaximumUploadSize = 0u;
constexpr std::chrono::milliseconds kXRootDTimeout{10 * 1000};

class XRootDHelperProxy {
public:
    XRootDHelperProxy(std::string url)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              kXRootDHelperThreadCount)}
    {
        using namespace one::helpers;

        std::unordered_map<folly::fbstring, folly::fbstring> params;
        params["url"] = url;
        params["credentialsType"] = "none";
        params["credentials"] = "admin:password";
        params["timeout"] = std::to_string(kXRootDTimeout.count());

        m_helper =
            std::make_shared<XRootDHelper>(XRootDHelperParams::create(params),
                m_executor, ExecutionContext::ONECLIENT);
    }

    struct stat getattr(std::string fileId)
    {
        ReleaseGIL guard;
        return m_helper->getattr(fileId).get();
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

    ReadDirResult readdir(std::string fileId, int offset, int count)
    {
        ReleaseGIL guard;
        std::vector<std::string> res;
        for (auto &direntry : m_helper->readdir(fileId, offset, count).get()) {
            res.emplace_back(direntry.toStdString());
        }
        return res;
    }

    void rename(std::string from, std::string to)
    {
        ReleaseGIL guard;
        m_helper->rename(from, to).get();
    }

    auto read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, O_RDONLY, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                return handle->read(offset, size)
                    .thenValue([handle](folly::IOBufQueue &&buf) {
                        return handle->release().thenValue(
                            [handle, buf = std::move(buf)](auto && /*unit*/) {
                                std::string data;
                                buf.appendToString(data);
                                return boost::python::api::object(
                                    boost::python::handle<>(
                                        PyBytes_FromStringAndSize(
                                            data.c_str(), data.size())));
                            });
                    });
            })
            .get();
    }

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;

        auto mknodLambda = [=](auto && /*unit*/) {
            return m_helper->mknod(fileId, S_IFREG | 0666, {}, 0);
        };
        auto writeLambda = [=](auto && /*unit*/) {
            return m_helper->open(fileId, O_WRONLY, {})
                .thenValue([=](one::helpers::FileHandlePtr &&handle) {
                    folly::IOBufQueue buf{
                        folly::IOBufQueue::cacheChainLength()};
                    buf.append(data);
                    return handle->write(offset, std::move(buf), {})
                        .thenValue([handle](std::size_t &&size) {
                            return handle->release().thenValue(
                                [handle, size](
                                    auto && /*unit*/) { return size; });
                        });
                });
        };

        return m_helper->access(fileId, 0)
            .thenValue(writeLambda)
            .thenError(folly::tag_t<std::exception>{},
                [mknodLambda = std::move(mknodLambda),
                    writeLambda = std::move(writeLambda),
                    executor = m_executor](auto &&e) {
                    return folly::makeSemiFuture()
                        .via(executor.get())
                        .thenValue(mknodLambda)
                        .thenValue(writeLambda);
                })
            .get();
    }

    void truncate(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, offset, size).get();
    }

    void mkdir(std::string fileId, mode_t mode)
    {
        ReleaseGIL guard;
        m_helper->mkdir(fileId, mode).get();
    }

    void mknod(
        std::string fileId, mode_t mode, std::vector<one::helpers::Flag> flags)
    {
        ReleaseGIL guard;
        m_helper
            ->mknod(fileId, mode,
                one::helpers::FlagsSet(flags.begin(), flags.end()), 0)
            .get();
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
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    std::shared_ptr<one::helpers::XRootDHelper> m_helper;
};

namespace {
auto create(std::string url)
{
    return boost::make_shared<XRootDHelperProxy>(std::move(url));
}
} // namespace

BOOST_PYTHON_MODULE(xrootd_helper)
{
    class_<XRootDHelperProxy, boost::noncopyable>("XRootDHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("getattr", &XRootDHelperProxy::getattr)
        .def("unlink", &XRootDHelperProxy::unlink)
        .def("rmdir", &XRootDHelperProxy::rmdir)
        .def("readdir", &XRootDHelperProxy::readdir)
        .def("rename", &XRootDHelperProxy::rename)
        .def("read", &XRootDHelperProxy::read)
        .def("write", &XRootDHelperProxy::write)
        .def("mkdir", &XRootDHelperProxy::mkdir)
        .def("mknod", &XRootDHelperProxy::mknod)
        .def("truncate", &XRootDHelperProxy::truncate)
        .def("getxattr", &XRootDHelperProxy::getxattr)
        .def("setxattr", &XRootDHelperProxy::setxattr)
        .def("removexattr", &XRootDHelperProxy::removexattr)
        .def("listxattr", &XRootDHelperProxy::listxattr);
}
