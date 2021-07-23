/**
 * @file bufferedNullDeviceHelperProxy.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nullDeviceHelper.h"

#include "buffering/bufferAgent.h"

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

using BufferedFileHandlePtr =
    std::shared_ptr<one::helpers::buffering::BufferedFileHandle>;

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

class BufferedNullDeviceHelperProxy {
public:
    BufferedNullDeviceHelperProxy(const int latencyMin, const int latencyMax,
        const double timeoutProbability, std::string filter)
        : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(
              NULL_DEVICE_HELPER_WORKER_THREADS,
              std::make_shared<StorageWorkerFactory>("null_t"))}
        , m_scheduler{std::make_shared<one::Scheduler>(1)}
        , m_helper{std::make_shared<one::helpers::buffering::BufferAgent>(
              one::helpers::buffering::BufferLimits{},
              std::make_shared<one::helpers::NullDeviceHelper>(latencyMin,
                  latencyMax, timeoutProbability, std::move(filter),
                  std::vector<std::pair<long int, long int>>{}, 0.0, 1024,
                  m_executor),
              m_scheduler,
              std::make_shared<
                  one::helpers::buffering::BufferAgentsMemoryLimitGuard>(
                  one::helpers::buffering::BufferLimits{}))}
    {
    }

    ~BufferedNullDeviceHelperProxy() {}

    BufferedFileHandlePtr open(std::string fileId, int flags)
    {
        ReleaseGIL guard;
        auto handle = m_helper->open(fileId, flags, {}).get();
        return std::dynamic_pointer_cast<
            one::helpers::buffering::BufferedFileHandle>(handle);
    }

    int readBytes(BufferedFileHandlePtr handle)
    {
        auto nullDeviceHandle =
            std::dynamic_pointer_cast<one::helpers::NullDeviceFileHandle>(
                handle->wrappedHandle());

        assert(nullDeviceHandle.get() != nullptr);

        return nullDeviceHandle->readBytes();
    }

    int writtenBytes(BufferedFileHandlePtr handle)
    {
        auto nullDeviceHandle =
            std::dynamic_pointer_cast<one::helpers::NullDeviceFileHandle>(
                handle->wrappedHandle());

        assert(nullDeviceHandle.get() != nullptr);

        return nullDeviceHandle->writtenBytes();
    }

    std::string read(BufferedFileHandlePtr handle, int offset, int size)
    {
        ReleaseGIL guard;
        return handle->read(offset, size)
            .thenValue([handle](folly::IOBufQueue &&buf) {
                std::string data;
                buf.appendToString(data);
                return data;
            })
            .get();
    }

    int write(BufferedFileHandlePtr handle, std::string data, int offset)
    {
        ReleaseGIL guard;
        folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
        buf.append(data);
        return handle->write(offset, std::move(buf), {})
            .thenValue([handle](int &&size) { return size; })
            .get();
    }

    void mknod(std::string fileId, mode_t mode, std::vector<Flag> flags)
    {
        ReleaseGIL guard;
        m_helper->mknod(fileId, mode, FlagsSet(flags.begin(), flags.end()), 0)
            .get();
    }

    void unlink(std::string fileId)
    {
        ReleaseGIL guard;
        m_helper->unlink(fileId, 0).get();
    }

    void truncate(std::string fileId, int offset)
    {
        ReleaseGIL guard;
        m_helper->truncate(fileId, offset, 0).get();
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
    std::shared_ptr<one::Scheduler> m_scheduler;
    std::shared_ptr<one::helpers::buffering::BufferAgent> m_helper;
};

namespace {
boost::shared_ptr<BufferedNullDeviceHelperProxy> create(const int latencyMin,
    const int latencyMax, const double timeoutProbability, std::string filter)
{
    return boost::make_shared<BufferedNullDeviceHelperProxy>(
        latencyMin, latencyMax, timeoutProbability, std::move(filter));
}
} // namespace

BOOST_PYTHON_MODULE(readcache_prefetch)
{
    class_<BufferedFileHandlePtr>("BufferedFileHandle", no_init);

    class_<BufferedNullDeviceHelperProxy, boost::noncopyable>(
        "BufferedNullDeviceHelperProxy", no_init)
        .def("__init__", make_constructor(create))
        .def("open", &BufferedNullDeviceHelperProxy::open)
        .def("read", &BufferedNullDeviceHelperProxy::read)
        .def("write", &BufferedNullDeviceHelperProxy::write)
        .def("mknod", &BufferedNullDeviceHelperProxy::mknod)
        .def("truncate", &BufferedNullDeviceHelperProxy::truncate)
        .def("unlink", &BufferedNullDeviceHelperProxy::unlink)
        .def("writtenBytes", &BufferedNullDeviceHelperProxy::writtenBytes)
        .def("readBytes", &BufferedNullDeviceHelperProxy::readBytes);
}
