/**
 * @file cephHelper.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephHelper.h"
#include "logging.h"
#include "monitoring/monitoring.h"

#include <glog/stl_logging.h>

#include <functional>

namespace one {
namespace helpers {

using namespace std::placeholders;

// Retry only in case one of these errors occured
const std::set<int> CEPH_RETRY_ERRORS = {EINTR, EIO, EAGAIN, EACCES, EBUSY,
    EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK, EWOULDBLOCK, ENONET,
    ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN, ENETUNREACH, ECONNABORTED,
    ECONNRESET, ENOTCONN, EHOSTDOWN, EHOSTUNREACH, EREMOTEIO, ENOMEDIUM,
    ECANCELED};

inline bool CephRetryCondition(int result, const std::string &operation)
{
    auto ret = (CEPH_RETRY_ERRORS.find(-result) == CEPH_RETRY_ERRORS.end());

    if (!ret) {
        LOG(WARNING) << "Retrying Ceph helper operation '" << operation
                     << "' due to error: " << result;
        ONE_METRIC_COUNTER_INC(
            "comp.helpers.mod.ceph." + operation + ".retries");
    }

    return ret;
}

CephFileHandle::CephFileHandle(folly::fbstring fileId,
    std::shared_ptr<CephHelper> helper, librados::IoCtx &ioCTX)
    : FileHandle{std::move(fileId)}
    , m_helper{std::move(helper)}
    , m_ioCTX{ioCTX}
{
    LOG_FCALL() << LOG_FARG(fileId);
}

folly::Future<folly::IOBufQueue> CephFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.ceph.read");

    return m_helper->connect().then([
        this, offset, size,
        s = std::weak_ptr<CephFileHandle>{shared_from_this()},
        timer = std::move(timer)
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::IOBufQueue>(ECANCELED);

        folly::IOBufQueue buffer{folly::IOBufQueue::cacheChainLength()};
        char *raw = static_cast<char *>(buffer.preallocate(size, size).first);
        librados::bufferlist data;
        libradosstriper::RadosStriper &rs = m_helper->getRadosStriper();

        LOG_DBG(2) << "Attempting to read " << size << " bytes at offset "
                   << offset << " from file " << m_fileId;

        auto ret = retry(
            [&]() {
                return rs.read(m_fileId.toStdString(), &data, size, offset);
            },
            std::bind(CephRetryCondition, _1, "read"));

        if (ret < 0) {
            LOG_DBG(1) << "Read failed from " << m_fileId
                       << " with error:" << ret;
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.ceph.errors.read");
            return makeFuturePosixException<folly::IOBufQueue>(ret);
        }

        LOG_DBG(2) << "Read " << ret << " bytes at offset " << offset
                   << " from file " << m_fileId;

        data.copy(0, ret, raw);
        buffer.postallocate(ret);

        ONE_METRIC_TIMERCTX_STOP(timer, ret);

        return folly::makeFuture(std::move(buffer));
    });
}

folly::Future<std::size_t> CephFileHandle::write(
    const off_t offset, folly::IOBufQueue buf)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.ceph.write");

    return m_helper->connect().then([
        this, buf = std::move(buf), offset,
        s = std::weak_ptr<CephFileHandle>{shared_from_this()},
        timer = std::move(timer)
    ]() mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<std::size_t>(ECANCELED);

        auto size = buf.chainLength();
        librados::bufferlist data;

        for (auto &byteRange : *buf.front())
            data.append(ceph::buffer::create_static(byteRange.size(),
                reinterpret_cast<char *>(
                    const_cast<unsigned char *>(byteRange.data()))));

        LOG_DBG(2) << "Attempting to write " << size << " bytes at offset "
                   << offset << " to file " << m_fileId;
        libradosstriper::RadosStriper &rs = m_helper->getRadosStriper();

        auto ret = retry([&, data = std::move(data) ]() {
            return rs.write(m_fileId.toStdString(), data, size, offset);
        },
            std::bind(CephRetryCondition, _1, "write"));

        if (ret < 0) {
            LOG_DBG(1) << "Write failed to" << m_fileId
                       << " with error:" << ret;
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.ceph.errors.write");
            return makeFuturePosixException<std::size_t>(ret);
        }

        LOG_DBG(2) << "Written " << ret << " bytes at offset " << offset
                   << " to file " << m_fileId;

        ONE_METRIC_TIMERCTX_STOP(timer, size);

        return folly::makeFuture(size);
    });
}

const Timeout &CephFileHandle::timeout() { return m_helper->timeout(); }

CephHelper::CephHelper(folly::fbstring clusterName, folly::fbstring monHost,
    folly::fbstring poolName, folly::fbstring userName, folly::fbstring key,
    std::unique_ptr<folly::Executor> executor, Timeout timeout)
    : m_clusterName{std::move(clusterName)}
    , m_monHost{std::move(monHost)}
    , m_poolName{std::move(poolName)}
    , m_userName{std::move(userName)}
    , m_key{std::move(key)}
    , m_executor{std::move(executor)}
    , m_timeout{std::move(timeout)}
{
    LOG_FCALL() << LOG_FARG(clusterName) << LOG_FARG(monHost)
                << LOG_FARG(poolName) << LOG_FARG(userName) << LOG_FARG(key);
}

CephHelper::~CephHelper()
{
    LOG_FCALL();
    m_ioCTX.close();
}

folly::Future<FileHandlePtr> CephHelper::open(
    const folly::fbstring &fileId, const int, const Params &)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto handle =
        std::make_shared<CephFileHandle>(fileId, shared_from_this(), m_ioCTX);

    return folly::makeFuture(std::move(handle));
}

folly::Future<folly::Unit> CephHelper::unlink(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().then(
        [ this, fileId, s = std::weak_ptr<CephHelper>{shared_from_this()} ] {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException(ECANCELED);

            LOG_DBG(2) << "Attempting to remove file " << fileId;

            auto ret = retry(
                [&]() { return m_radosStriper.remove(fileId.toStdString()); },
                std::bind(CephRetryCondition, _1, "remove"));

            if (ret == -EBUSY) {
                // If there are any dangling locks on the file, forcibly remove
                // the locks and attempt to unlink the file one more time
                if (removeStriperLocks(fileId) == 0) {
                    ret = m_radosStriper.remove(fileId.toStdString());
                }
            }

            if (ret < 0) {
                LOG(WARNING)
                    << "Removing file " << fileId << " failed: " << ret;
                return makeFuturePosixException(ret);
            }

            LOG_DBG(2) << "Removed file " << fileId;

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> CephHelper::truncate(
    const folly::fbstring &fileId, off_t size)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.ceph.truncate");

    return connect().then([
        this, size, fileId, s = std::weak_ptr<CephHelper>{shared_from_this()},
        timer = std::move(timer)
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException(ECANCELED);

        LOG_DBG(2) << "Attempting to truncate file " << fileId << " to size "
                   << size;

        auto ret = retry(
            [&]() { return m_radosStriper.trunc(fileId.toStdString(), size); },
            std::bind(CephRetryCondition, _1, "trunc"));

        if (ret == -ENOENT) {
            // libradosstripe will not truncate non-existing file, so we have to
            // create one first
            LOG_DBG(1) << "Attempting to truncate non-existing file - we have "
                          "to create it first";

            librados::bufferlist bl;
            m_radosStriper.write_full(fileId.toStdString(), bl);
            ret = m_radosStriper.trunc(fileId.toStdString(), size);
            ONE_METRIC_TIMERCTX_STOP(timer, size);
        }
        else if (ret == -EBUSY) {
            // If there are any dangling locks on the file, forcibly remove
            // the locks and attempt to truncate the file one more time
            if (removeStriperLocks(fileId) == 0) {
                ret = m_radosStriper.trunc(fileId.toStdString(), size);
            }
        }

        if (ret < 0) {
            LOG(WARNING) << "Truncating file " << fileId << " failed: " << ret;
            return makeFuturePosixException(ret);
        }

        LOG_DBG(2) << "Truncated file " << fileId;

        return folly::makeFuture();
    });
}

folly::Future<folly::fbstring> CephHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return connect().then([
        this, fileId, name, s = std::weak_ptr<CephHelper>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::fbstring>(ECANCELED);

        std::string xattrValue;

        LOG_DBG(2) << "Getting extended attribute " << name << " from file "
                   << fileId;

        librados::bufferlist bl;
        auto ret = retry(
            [&]() {
                return m_radosStriper.getxattr(
                    fileId.toStdString(), name.c_str(), bl);
            },
            std::bind(CephRetryCondition, _1, "getxattr"));

        if (ret < 0) {
            LOG_DBG(1) << "Getting extended attribute failed: " << ret;
            return makeFuturePosixException<folly::fbstring>(ret);
        }

        bl.copy(0, ret, xattrValue);

        LOG_DBG(2) << "Got extended attribute with value: " << xattrValue;

        return folly::makeFuture<folly::fbstring>(std::move(xattrValue));
    });
}

folly::Future<folly::Unit> CephHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name) << LOG_FARG(value)
                << LOG_FARG(create) << LOG_FARG(replace);

    return connect().then([
        this, fileId, name, value, create, replace,
        s = std::weak_ptr<CephHelper>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException(ECANCELED);

        if (create && replace) {
            LOG_DBG(1) << "Invalid combination of create and replace flags in "
                          "setxattr for "
                       << fileId;
            return makeFuturePosixException<folly::Unit>(EINVAL);
        }

        librados::bufferlist bl;

        if (create) {
            LOG_DBG(2) << "Checking if extended attribute " << name
                       << " already exists for file " << fileId
                       << " before creating";

            auto xattrExists = retry(
                [&]() {
                    return m_radosStriper.getxattr(
                        fileId.toStdString(), name.c_str(), bl);
                },
                std::bind(CephRetryCondition, _1, "getxattr"));

            if (xattrExists >= 0) {
                LOG_DBG(1) << "Extended attribute " << name
                           << " already exists for " << fileId
                           << " - cannot create again. Use 'replace' instead.";
                return makeFuturePosixException<folly::Unit>(EEXIST);
            }
        }
        else if (replace) {
            LOG_DBG(2) << "Checking if extended attribute " << name
                       << " already exists for file " << fileId
                       << " before replacing";

            auto xattrExists = retry(
                [&]() {
                    return m_radosStriper.getxattr(
                        fileId.toStdString(), name.c_str(), bl);
                },
                std::bind(CephRetryCondition, _1, "getxattr"));

            if (xattrExists < 0) {
                LOG_DBG(1) << "Extended attribute " << name
                           << " does not exist for " << fileId
                           << " - cannot replace. Use 'create' instead.";
                return makeFuturePosixException<folly::Unit>(ENODATA);
            }
        }

        bl.clear();
        bl.append(value.toStdString());

        LOG_DBG(2) << "Attempting to set extended attribute " << name
                   << " for file " << fileId;

        auto ret = retry(
            [&]() {
                return m_radosStriper.setxattr(
                    fileId.toStdString(), name.c_str(), bl);
            },
            std::bind(CephRetryCondition, _1, "setxattr"));

        if (ret < 0) {
            LOG_DBG(1) << "Failed to set extended attribute " << name
                       << " for file " << fileId << " with error: " << ret;
            return makeFuturePosixException<folly::Unit>(ret);
        }

        LOG_DBG(2) << "Set extended attribute " << name << " for file "
                   << fileId;

        return folly::makeFuture();
    });
}

folly::Future<folly::Unit> CephHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return connect().then([
        this, fileId, name, s = std::weak_ptr<CephHelper>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException(ECANCELED);

        LOG_DBG(2) << "Attempting to remove extended attribute " << name
                   << " for file " << fileId;

        auto ret = retry(
            [&]() {
                return m_radosStriper.rmxattr(
                    fileId.toStdString(), name.c_str());
            },
            std::bind(CephRetryCondition, _1, "rmxattr"));

        if (ret < 0) {
            LOG_DBG(1) << "Failed to remove extended attribute " << name
                       << " for file " << fileId << " with error: " << ret;
            return makeFuturePosixException<folly::Unit>(ret);
        }

        LOG_DBG(2) << "Removed extended attribute " << name << " from file "
                   << fileId;

        return folly::makeFuture();
    });
}

folly::Future<folly::fbvector<folly::fbstring>> CephHelper::listxattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().then([
        this, fileId, s = std::weak_ptr<CephHelper>{shared_from_this()}
    ] {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                ECANCELED);

        std::map<std::string, librados::bufferlist> xattrs;

        LOG_DBG(2) << "Attempting to list extended attributes for file "
                   << fileId;

        auto ret = retry(
            [&]() {
                return m_radosStriper.getxattrs(fileId.toStdString(), xattrs);
            },
            std::bind(CephRetryCondition, _1, "getxattrs"));

        if (ret < 0) {
            LOG_DBG(1) << "Failed retrieving extended attributes for file "
                       << fileId;
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                ret);
        }

        folly::fbvector<folly::fbstring> xattrNames;
        for (auto const &xattr : xattrs) {
            xattrNames.push_back(xattr.first);
        }

        LOG_DBG(2) << "Got extended attributes for file " << fileId << ": "
                   << LOG_VEC(xattrNames);

        return folly::makeFuture<folly::fbvector<folly::fbstring>>(
            std::move(xattrNames));
    });
}

folly::Future<folly::Unit> CephHelper::connect()
{
    LOG_FCALL();

    return folly::via(m_executor.get(),
        [ this, s = std::weak_ptr<CephHelper>{shared_from_this()} ] {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException(ECANCELED);

            std::lock_guard<std::mutex> guard{m_connectionMutex};

            LOG_DBG(2) << "Attempting to connect to Ceph server at: "
                       << m_monHost;

            if (m_connected) {
                return folly::makeFuture();
            }

            auto ret =
                m_cluster.init2(m_userName.c_str(), m_clusterName.c_str(), 0);
            if (ret < 0) {
                LOG(ERROR) << "Couldn't initialize the cluster handle.";
                return makeFuturePosixException(ret);
            }

            ret = m_cluster.conf_set("mon host", m_monHost.c_str());
            if (ret < 0) {
                LOG(ERROR) << "Couldn't set monitor host configuration "
                              "variable.";
                return makeFuturePosixException(ret);
            }

            ret = m_cluster.conf_set("key", m_key.c_str());
            if (ret < 0) {
                LOG(ERROR) << "Couldn't set key configuration variable.";
                return makeFuturePosixException(ret);
            }

            ret = retry([&]() { return m_cluster.connect(); },
                std::bind(CephRetryCondition, _1, "connect"));
            if (ret < 0) {
                LOG(ERROR) << "Couldn't connect to cluster.";
                return makeFuturePosixException(ret);
            }

            ret = retry(
                [&]() {
                    return m_cluster.ioctx_create(m_poolName.c_str(), m_ioCTX);
                },
                std::bind(CephRetryCondition, _1, "ioctx_create"));
            if (ret < 0) {
                LOG(ERROR) << "Couldn't set up ioCTX.";
                return makeFuturePosixException(ret);
            }

            // Setup libradosstriper context
            ret = retry(
                [&]() {
                    return libradosstriper::RadosStriper::striper_create(
                        m_ioCTX, &m_radosStriper);
                },
                std::bind(CephRetryCondition, _1, "striper_create"));
            if (ret < 0) {
                LOG(ERROR) << "Couldn't Create RadosStriper: " << ret;

                m_ioCTX.close();
                m_cluster.shutdown();
                return makeFuturePosixException(ret);
            }

            // Get the alignment setting for pool from io_ctx
            uint64_t alignment = 0;
            ret = m_ioCTX.pool_required_alignment2(&alignment);
            if (ret < 0) {
                LOG(ERROR) << "IO_CTX didn't return pool alignment: " << ret
                           << "\n Is this an erasure coded pool? " << std::endl;

                m_ioCTX.close();
                m_cluster.shutdown();
                return makeFuturePosixException(ret);
            }

            // TODO: VFS-3780
            m_radosStriper.set_object_layout_stripe_unit(4 * 1024 * 1024);
            m_radosStriper.set_object_layout_stripe_count(8);
            m_radosStriper.set_object_layout_object_size(16 * 1024 * 1024);

            LOG_DBG(1) << "Successfully connected to Ceph at: " << m_monHost;

            m_connected = true;
            return folly::makeFuture();
        });
}

int CephHelper::removeStriperLocks(const folly::fbstring &fileId)
{
    int exclusive;
    std::string tag;
    std::list<librados::locker_t> lockers;

    auto striperFileId =
        fileId.toStdString() + CEPH_STRIPER_FIRST_OBJECT_SUFFIX;
    auto result = m_ioCTX.list_lockers(
        striperFileId, CEPH_STRIPER_LOCK_NAME, &exclusive, &tag, &lockers);

    if (result < 0) {
        LOG(ERROR) << "Cannot list striper locks on " << striperFileId
                   << " due to error: " << result;
        return result;
    }

    for (const auto &locker : lockers) {
        result = m_ioCTX.break_lock(striperFileId, CEPH_STRIPER_LOCK_NAME,
            locker.client, locker.cookie);
        if (result < 0) {
            LOG(ERROR) << "Cannot break lock on " << striperFileId
                       << " due to error: " << result;
            return result;
        }
    }

    return 0;
}
} // namespace helpers
} // namespace one
