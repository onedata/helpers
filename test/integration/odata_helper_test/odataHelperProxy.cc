/*
 * odataHelperProxy.cc
 *
 *  Created on: 11. 4. 2018
 *      Author: jakub
 */
#include "odataHelper.h"

#include "asioExecutor.h"
#include <asio/buffer.hpp>
#include <asio/executor_work.hpp>
#include <asio/io_service.hpp>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/raw_function.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

using namespace boost::python;
using namespace one::helpers;

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

constexpr int ODATA_HELPER_WORKER_THREADS = 4;

class ODataHelperProxy {
public:
    ODataHelperProxy(const std::string &url)
        : m_service{ODATA_HELPER_WORKER_THREADS}
        , m_idleWork{asio::make_work(m_service)}
        , m_helper{std::make_shared<one::helpers::ODataHelper>(
              std::make_shared<one::AsioExecutor>(m_service), url, "testuser",
              "testpassword", "Sentinel-2,Sentinel-3", "products.db", ".", 10)}
    {
        for (int i = 0; i < ODATA_HELPER_WORKER_THREADS; i++) {
            m_workers.push_back(std::thread([=]() { m_service.run(); }));
        }
    }
    ~ODataHelperProxy()
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

        return -1;
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

    std::vector<std::string> readdir(std::string fileId, int offset, int count)
    {
        ReleaseGIL guard;
        std::vector<std::string> res;
        const auto result = m_helper->readdir(fileId, offset, count).get();
        for (auto &direntry : result) {
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

    void unlink(std::string fileId)
    {
        ReleaseGIL guard;
        m_helper->unlink(fileId).get();
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

    void truncate(std::string fileId, int offset)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, offset).get();
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

    bool isReady()
    {
        try {
            const auto result = m_helper->readdir("/Sentinel-2", 0, 5).get();
            return !result.empty();
        }
        catch (...) {
            return false;
        }
    }

private:
    asio::io_service m_service;
    asio::executor_work<asio::io_service::executor_type> m_idleWork;
    std::vector<std::thread> m_workers;
    std::shared_ptr<one::helpers::ODataHelper> m_helper;
};

namespace {
boost::shared_ptr<ODataHelperProxy> create(std::string url)
{
    return boost::make_shared<ODataHelperProxy>(url);
}

class CleanUp {
public:
    CleanUp() = default;
    ~CleanUp() { std::remove("products.db"); }
};

CleanUp clean_up;

} // namespace

BOOST_PYTHON_MODULE(odata_helper)
{
    class_<ODataHelperProxy, boost::noncopyable>("ODataHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("open", &ODataHelperProxy::open)
        .def("read", &ODataHelperProxy::read)
        .def("write", &ODataHelperProxy::write)
        .def("getattr", &ODataHelperProxy::getattr)
        .def("readdir", &ODataHelperProxy::readdir)
        .def("readlink", &ODataHelperProxy::readlink)
        .def("mknod", &ODataHelperProxy::mknod)
        .def("mkdir", &ODataHelperProxy::mkdir)
        .def("unlink", &ODataHelperProxy::unlink)
        .def("rmdir", &ODataHelperProxy::rmdir)
        .def("symlink", &ODataHelperProxy::symlink)
        .def("rename", &ODataHelperProxy::rename)
        .def("link", &ODataHelperProxy::link)
        .def("chmod", &ODataHelperProxy::chmod)
        .def("chown", &ODataHelperProxy::chown)
        .def("truncate", &ODataHelperProxy::truncate)
        .def("getxattr", &ODataHelperProxy::getxattr)
        .def("setxattr", &ODataHelperProxy::setxattr)
        .def("removexattr", &ODataHelperProxy::removexattr)
        .def("listxattr", &ODataHelperProxy::listxattr)
        .def("access", &ODataHelperProxy::access)
        .def("isReady", &ODataHelperProxy::isReady);
}
