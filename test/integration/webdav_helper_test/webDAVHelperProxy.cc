/**
 * @file webDAVHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "posixHelper.h"
#include "webDAVHelper.h"

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

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

constexpr auto kWebDAVHelperThreadCount = 5u;
constexpr auto kWebDAVConnectionPoolSize = 10u;
constexpr auto kWebDAVMaximumUploadSize = 0u;

class WebDAVHelperProxy {
public:
    WebDAVHelperProxy(std::string endpoint, std::string credentials)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              kWebDAVHelperThreadCount)}
        , m_helper{std::make_shared<one::helpers::WebDAVHelper>(
              Poco::URI(endpoint), true,
              one::helpers::WebDAVCredentialsType::BASIC, credentials, "",
              one::helpers::WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE,
              kWebDAVConnectionPoolSize, kWebDAVMaximumUploadSize, m_executor)}
    {
    }

    ~WebDAVHelperProxy() {}

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

    std::string read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .then([&](one::helpers::FileHandlePtr handle) {
                return handle->read(offset, size);
            })
            .then([](folly::IOBufQueue &&buf) {
                std::string data;
                buf.appendToString(data);
                return data;
            })
            .get();
    }

    int write(std::string fileId, std::string data, int offset)
    {
        ReleaseGIL guard;

        auto mknodLambda = [&] {
            return m_helper->mknod(fileId, S_IFREG | 0666, {}, 0);
        };
        auto writeLambda = [&] {
            return m_helper->open(fileId, O_WRONLY, {})
                .then([&](one::helpers::FileHandlePtr handle) {
                    folly::IOBufQueue buf{
                        folly::IOBufQueue::cacheChainLength()};
                    buf.append(data);
                    return handle->write(offset, std::move(buf))
                        .then([handle](std::size_t size) {
                            handle->flush();
                            return size;
                        });
                });
        };

        return m_helper->access(fileId, 0)
            .then(writeLambda)
            .onError([
                mknodLambda = std::move(mknodLambda),
                writeLambda = std::move(writeLambda), executor = m_executor
            ](std::exception const &e) {
                return mknodLambda().then(writeLambda);
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
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::WebDAVHelper> m_helper;
};

namespace {
boost::shared_ptr<WebDAVHelperProxy> create(
    std::string endpoint, std::string credentials)
{
    return boost::make_shared<WebDAVHelperProxy>(
        std::move(endpoint), std::move(credentials));
}
} // namespace

BOOST_PYTHON_MODULE(webdav_helper)
{
    class_<WebDAVHelperProxy, boost::noncopyable>("WebDAVHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("getattr", &WebDAVHelperProxy::getattr)
        .def("unlink", &WebDAVHelperProxy::unlink)
        .def("rmdir", &WebDAVHelperProxy::rmdir)
        .def("readdir", &WebDAVHelperProxy::readdir)
        .def("rename", &WebDAVHelperProxy::rename)
        .def("read", &WebDAVHelperProxy::read)
        .def("write", &WebDAVHelperProxy::write)
        .def("mkdir", &WebDAVHelperProxy::mkdir)
        .def("mknod", &WebDAVHelperProxy::mknod)
        .def("truncate", &WebDAVHelperProxy::truncate)
        .def("getxattr", &WebDAVHelperProxy::getxattr)
        .def("setxattr", &WebDAVHelperProxy::setxattr)
        .def("removexattr", &WebDAVHelperProxy::removexattr)
        .def("listxattr", &WebDAVHelperProxy::listxattr);
}
