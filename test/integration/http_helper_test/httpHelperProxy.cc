/**
 * @file httpHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "httpHelper.h"
#include "posixHelper.h"

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

class ReleaseGIL {
public:
    ReleaseGIL()
        : threadState{PyEval_SaveThread(), PyEval_RestoreThread}
    {
    }

private:
    std::unique_ptr<PyThreadState, decltype(&PyEval_RestoreThread)> threadState;
};

constexpr auto kHTTPHelperThreadCount = 5u;
constexpr auto kHTTPConnectionPoolSize = 5u;
// constexpr auto kHTTPMaximumUploadSize = 0u;

class HTTPHelperProxy {
public:
    HTTPHelperProxy(std::string endpoint, std::string credentials,
        std::string credentialsType)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              kHTTPHelperThreadCount)}
    {
        using namespace one::helpers;

        std::unordered_map<folly::fbstring, folly::fbstring> params;
        params["endpoint"] = endpoint;
        params["verifyServerCertificate"] = "false";
        params["credentialsType"] = credentialsType;
        params["credentials"] = credentials;
        params["connectionPoolSize"] = std::to_string(kHTTPConnectionPoolSize);

        m_helper = std::make_shared<HTTPHelper>(
            HTTPHelperParams::create(params), m_executor);
    }

    ~HTTPHelperProxy() { }

    void checkStorageAvailability()
    {
        ReleaseGIL guard;
        m_helper->checkStorageAvailability().get();
    }

    struct stat getattr(std::string fileId)
    {
        ReleaseGIL guard;
        return m_helper->getattr(fileId).get();
    }

    auto read(std::string fileId, int offset, int size)
    {
        ReleaseGIL guard;
        return m_helper->open(fileId, 0, {})
            .thenValue([&](one::helpers::FileHandlePtr &&handle) {
                return handle->read(offset, size);
            })
            .thenValue([](folly::IOBufQueue &&buf) {
                std::string data;
                buf.appendToString(data);
                return boost::python::api::object(boost::python::handle<>(
                    PyBytes_FromStringAndSize(data.c_str(), data.size())));
            })
            .get();
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::helpers::HTTPHelper> m_helper;
};

namespace {
boost::shared_ptr<HTTPHelperProxy> create(
    std::string endpoint, std::string credentials, std::string credentialsType)
{
    return boost::make_shared<HTTPHelperProxy>(std::move(endpoint),
        std::move(credentials), std::move(credentialsType));
}
} // namespace

BOOST_PYTHON_MODULE(http_helper)
{
    class_<HTTPHelperProxy, boost::noncopyable>("HTTPHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("getattr", &HTTPHelperProxy::getattr)
        .def("read", &HTTPHelperProxy::read)
        .def("check_storage_availability",
            &HTTPHelperProxy::checkStorageAvailability);
}
