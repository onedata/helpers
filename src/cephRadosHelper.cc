/**
 * @file cephRadosHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephRadosHelper.h"
#include "helpers/logging.h"
#include "helpers/storageHelper.h"
#include "monitoring/monitoring.h"

#include <boost/algorithm/string.hpp>
#include <folly/Range.h>
#include <glog/stl_logging.h>

#include <algorithm>
#include <functional>

namespace {

inline bool CephRadosRetryCondition(int result, const std::string &operation)
{
    // Retry only in case one of these errors occured
    const static std::set<int> CEPHRADOS_RETRY_ERRORS = {EINTR, EIO, EAGAIN,
        EACCES, EBUSY, EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK,
        EWOULDBLOCK, ENONET, ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN,
        ENETUNREACH, ECONNABORTED, ECONNRESET, ENOTCONN, EHOSTDOWN,
        EHOSTUNREACH, EREMOTEIO, ENOMEDIUM, ECANCELED};

    auto ret =
        (CEPHRADOS_RETRY_ERRORS.find(-result) == CEPHRADOS_RETRY_ERRORS.end());

    if (!ret) {
        LOG(WARNING) << "Retrying CephRados helper operation '" << operation
                     << "' due to error: " << result;
        ONE_METRIC_COUNTER_INC(
            "comp.helpers.mod.cephrados." + operation + ".retries");
    }

    return ret;
}

void throwOnError(const folly::fbstring &operation, const int code)
{
    if (code == 0)
        return;

    auto msg = std::string("Operation ") + operation.toStdString() +
        " failed with error " + std::to_string(code);

    LOG_DBG(1) << msg;

    if (operation == "PutObject") {
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.cephrados.errors.write");
    }
    else if (operation == "GetObject") {
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.cephrados.errors.read");
    }

    throw std::system_error{std::error_code(code, std::system_category()), msg};
}
} // namespace

namespace one {
namespace helpers {

using std::placeholders::_1;

CephRadosHelper::CephRadosHelper(folly::fbstring clusterName,
    folly::fbstring monHost, folly::fbstring poolName, folly::fbstring userName,
    folly::fbstring key, Timeout timeout, StoragePathType storagePathType)
    : KeyValueHelper{true, storagePathType}
    , m_clusterName{std::move(clusterName)}
    , m_monHost{std::move(monHost)}
    , m_poolName{std::move(poolName)}
    , m_userName{std::move(userName)}
    , m_key{std::move(key)}
    , m_timeout{timeout}
{
    LOG_FCALL() << LOG_FARG(m_clusterName) << LOG_FARG(m_monHost)
                << LOG_FARG(m_poolName) << LOG_FARG(m_userName)
                << LOG_FARG(m_timeout.count());
}

folly::IOBufQueue CephRadosHelper::getObject(
    const folly::fbstring &key, const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(offset) << LOG_FARG(size);

    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    log_timer<> logTimer;

    connect();

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
    char *raw = static_cast<char *>(buf.preallocate(size, size).first);

    librados::bufferlist data;

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.cephrados.read");

    LOG_DBG(2) << "Attempting to get " << size << "bytes from object " << key
               << " at offset " << offset;

    auto ret = retry(
        [&, this]() {
            return m_ctx->ioCTX.read(key.toStdString(), data, size, offset);
        },
        std::bind(CephRadosRetryCondition, _1, "GetObject"));

    // Treat non-existent object as empty read
    if (ret == -ENOENT) {
        LOG_DBG(2) << "Failed reading object " << key
                   << " - object does not exist.";
        ret = 0;
    }

    if (ret < 0) {
        LOG_DBG(1) << "Reading from object " << key << " failed with error "
                   << ret;
        throwOnError("GetObject", ret);
    }

    std::memcpy(raw, data.c_str(), static_cast<std::size_t>(ret));
    buf.postallocate(static_cast<std::size_t>(ret));

    log<read_write_perf>(
        key, "CephRadosHelper", "getObject", offset, size, logTimer.stop());

    LOG_DBG(2) << "Read " << ret << " bytes from object " << key;

    ONE_METRIC_TIMERCTX_STOP(timer, ret);

    return buf;
}

std::size_t CephRadosHelper::putObject(
    const folly::fbstring &key, folly::IOBufQueue buf, const std::size_t offset)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(buf.chainLength())
                << LOG_FARG(offset);

    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    log_timer<> logTimer;

    connect();

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.cephrados.write");

    auto size = buf.chainLength();
    librados::bufferlist data;
    for (const auto &byteRange : *buf.front())
        data.append(ceph::buffer::create_static(byteRange.size(),
            // NOLINTNEXTLINE
            reinterpret_cast<char *>(
                // NOLINTNEXTLINE
                const_cast<unsigned char *>(byteRange.data()))));

    LOG_DBG(2) << "Attempting to write object " << key << " of size " << size;

    auto ret = retry(
        [&]() {
            return m_ctx->ioCTX.write(key.toStdString(), data, size, offset);
        },
        std::bind(CephRadosRetryCondition, _1, "PutObject"));

    ONE_METRIC_TIMERCTX_STOP(timer, size);

    throwOnError("PutObject", ret);

    log<read_write_perf>(
        key, "CephRadosHelper", "putObject", offset, size, logTimer.stop());

    LOG_DBG(2) << "Written " << size << " bytes to object " << key;

    return size;
}

void CephRadosHelper::deleteObject(const folly::fbstring &key)
{
    LOG_FCALL() << LOG_FARG(key);

    connect();

    LOG_DBG(2) << "Attempting to delete object " << key;

    auto ret = retry([&]() { return m_ctx->ioCTX.remove(key.toStdString()); },
        std::bind(CephRadosRetryCondition, _1, "RemoveObject"));

    // Ignore non-existent object errors
    if (ret == -ENOENT) {
        LOG_DBG(2) << "Failed removing object " << key
                   << " - object does not exist.";
        ret = 0;
    }

    if (ret < 0)
        throwOnError("RemoveObject", ret);
}

void CephRadosHelper::deleteObjects(
    const folly::fbvector<folly::fbstring> & /*keys*/)
{
    throw std::system_error{
        std::make_error_code(std::errc::operation_not_supported)};
}

void CephRadosHelper::connect()
{
    std::lock_guard<std::mutex> guard{m_connectionMutex};

    if (m_ctx->connected)
        return;

    int ret =
        m_ctx->cluster.init2(m_userName.c_str(), m_clusterName.c_str(), 0);
    if (ret < 0) {
        LOG(ERROR) << "Couldn't initialize the cluster handle.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    ret = m_ctx->cluster.conf_set("mon host", m_monHost.c_str());
    if (ret < 0) {
        LOG(ERROR) << "Couldn't set monitor host configuration "
                      "variable.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    ret = m_ctx->cluster.conf_set("key", m_key.c_str());
    if (ret < 0) {
        LOG(ERROR) << "Couldn't set key configuration variable.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    const auto timeoutStr = std::to_string(m_timeout.count() / 1000);

    ret = m_ctx->cluster.conf_set("rados_osd_op_timeout", timeoutStr.c_str());
    if (ret < 0) {
        LOG(ERROR)
            << "Couldn't set rados_osd_op_timeout configuration variable.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    ret = m_ctx->cluster.conf_set("rados_mon_op_timeout", timeoutStr.c_str());
    if (ret < 0) {
        LOG(ERROR)
            << "Couldn't set rados_mon_op_timeout configuration variable.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    ret = m_ctx->cluster.conf_set("client_mount_timeout", timeoutStr.c_str());
    if (ret < 0) {
        LOG(ERROR)
            << "Couldn't set client_mount_timeout configuration variable.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    ret = m_ctx->cluster.connect();
    if (ret < 0) {
        LOG(ERROR) << "Couldn't connect to cluster.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    ret = m_ctx->cluster.ioctx_create(m_poolName.c_str(), m_ctx->ioCTX);
    if (ret < 0) {
        LOG(ERROR) << "Couldn't set up ioCTX.";
        throw std::system_error{one::helpers::makePosixError(ret)};
    }

    m_ctx->connected = true;
}
} // namespace helpers
} // namespace one
