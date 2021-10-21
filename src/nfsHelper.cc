/**
 * @file nfsHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nfsHelper.h"

#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <folly/futures/Retrying.h>
#include <nfsc/libnfs-raw.h>

namespace one {
namespace helpers {

const std::set<int> &NFSRetryErrors()
{
    static const std::set<int> NFS_RETRY_ERRORS = {EINTR, EIO, EAGAIN, EACCES,
        EBUSY, EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK, EWOULDBLOCK,
        ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN, ENETUNREACH, ECONNABORTED,
        ECONNRESET, ENOTCONN, EHOSTUNREACH, ECANCELED};
    return NFS_RETRY_ERRORS;
}

namespace {
class nfs_system_error : public std::system_error {
public:
    nfs_system_error(std::error_code ec, const std::string &op)
        : std::system_error{ec}
        , operation{op}
    {
    }

    std::string operation;
};

inline nfs_system_error makeNFSException(
    const int posixCode, const std::string &operation)
{
    return nfs_system_error{
        one::helpers::makePosixError(-posixCode), operation};
}

template <typename T = folly::Unit>
inline folly::Future<T> makeFutureNFSException(
    const int posixCode, const std::string &operation)
{
    return folly::makeFuture<T>(makeNFSException(posixCode, operation));
}
}

inline std::function<bool(size_t, const folly::exception_wrapper &)>
NFSRetryPolicy(size_t maxTries)
{
    return [maxTries](size_t n, const folly::exception_wrapper &ew) {
        const auto *e = ew.get_exception<nfs_system_error>();
        if (e == nullptr)
            return false;

        auto shouldRetry = (n < maxTries) &&
            ((e->code().value() >= 0) &&
                (NFSRetryErrors().find(e->code().value()) !=
                    NFSRetryErrors().end()));

        if (shouldRetry) {
            LOG(WARNING) << "Retrying NFS helper operation '" << e->operation
                         << "' due to error: " << e->code();
            ONE_METRIC_COUNTER_INC(
                "comp.helpers.mod.nfs." + e->operation + ".retries");
        }

        return shouldRetry;
    };
}

NFSFileHandle::NFSFileHandle(const folly::fbstring &fileId,
    std::shared_ptr<NFSHelper> helper, struct nfsfh *nfsFh,
    std::shared_ptr<folly::Executor> executor, Timeout timeout)
    : FileHandle{std::move(fileId), std::move(helper)}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
    , m_nfsFh{nfsFh}
{
}

NFSFileHandle::~NFSFileHandle() { }

folly::Future<folly::IOBufQueue> NFSFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.nfs.read");

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh, offset, size,
            timer = std::move(timer)](size_t retryCounter) {
            folly::IOBufQueue buffer{folly::IOBufQueue::cacheChainLength()};
            char *raw =
                static_cast<char *>(buffer.preallocate(size, size).first);

            LOG_DBG(2) << "Attempting to read " << size << " bytes at offset "
                       << offset << " from file " << fileId;

            auto ret = nfs_pread(nfs, nfsFh, offset, size, raw);

            if (ret < 0) {
                LOG_DBG(1) << "NFS read failed from " << fileId;

                return one::helpers::makeFutureNFSException<folly::IOBufQueue>(
                    ret, "read");
            }

            buffer.postallocate(ret);

            LOG_DBG(2) << "Read " << ret << " from file " << fileId;

            ONE_METRIC_TIMERCTX_STOP(timer, ret);

            return folly::makeFuture(std::move(buffer));
        })
        .via(executor().get());
}

folly::Future<std::size_t> NFSFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset);

    if (buf.empty())
        return folly::makeFuture<std::size_t>(0);

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.nfs.write");

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh, buf = std::move(buf), offset,
            writeCb = std::move(writeCb),
            timer = std::move(timer)](size_t retryCounter) mutable {
            const auto size = buf.chainLength();

            auto iobuf = buf.move();
            if (iobuf->isChained()) {
                LOG_DBG(2) << "Coalescing chained buffer at offset " << offset
                           << " of size: " << iobuf->length();
                iobuf->unshare();
                iobuf->coalesce();
            }

            auto ret =
                nfs_pwrite(nfs, nfsFh, offset, size, iobuf->writableData());

            if (ret < 0) {
                LOG_DBG(1) << "Failed to call async nfs write on " << fileId;

                return one::helpers::makeFutureNFSException<std::size_t>(
                    ret, "write");
            }

            if (writeCb)
                writeCb(ret);

            LOG_DBG(2) << "Written " << ret << " bytes to file " << fileId;

            ONE_METRIC_TIMERCTX_STOP(timer, ret);

            return folly::makeFuture<size_t>(ret);
        })
        .via(executor().get());
}

folly::Future<folly::Unit> NFSFileHandle::release()
{
    LOG_FCALL();

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh,
            s = std::weak_ptr<NFSFileHandle>{shared_from_this()}](
            size_t retryCounter) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto ret = nfs_close(nfs, nfsFh);

            if (ret != 0) {
                LOG_DBG(1) << "Failed to call async nfs close on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    ret, "release");
            }

            return folly::makeFuture();
        })
        .via(executor().get());
}

folly::Future<folly::Unit> NFSFileHandle::flush() { return {}; }

folly::Future<folly::Unit> NFSFileHandle::fsync(bool /*isDataSync*/)
{
    LOG_FCALL();

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh,
            s = std::weak_ptr<NFSFileHandle>{shared_from_this()}](
            size_t retryCounter) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto ret = nfs_fsync(nfs, nfsFh);

            if (ret != 0) {
                LOG_DBG(1) << "Failed to call async nfs fsync on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    ret, "fsync");
            }

            return folly::makeFuture();
        })
        .via(executor().get());
}

NFSHelper::NFSHelper(std::shared_ptr<NFSHelperParams> params,
    std::shared_ptr<folly::Executor> executor, Timeout timeout,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
{
    LOG_FCALL();

    invalidateParams()->setValue(std::move(params));
}

folly::Future<FileHandlePtr> NFSHelper::open(const folly::fbstring &fileId,
    const int flags, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(flags);

    return connect().thenValue(
        [this, fileId, flags, helper = shared_from_this(),
            executor = executor(), timeout = timeout()](auto && /*unit*/) {
            return folly::futures::retrying(
                NFSRetryPolicy(constants::IO_RETRY_COUNT),
                [this, fileId, flags, helper = std::move(helper), executor,
                    timeout](size_t retryCounter) {
                    struct nfsfh *nfsFh;

                    LOG_DBG(2) << "Attempting to open file " << fileId;

                    auto error = nfs_open(m_nfs, fileId.c_str(), flags, &nfsFh);

                    if (error != 0) {
                        LOG(ERROR) << "NFS open failed on " << fileId;

                        return one::helpers::makeFutureNFSException<
                            std::shared_ptr<NFSFileHandle>>(error, "open");
                    }

                    return folly::makeFuture<std::shared_ptr<NFSFileHandle>>(
                        std::make_shared<NFSFileHandle>(fileId,
                            std::move(helper), nfsFh, executor, timeout));
                })
                .via(executor.get());
        });
}

folly::Future<struct stat> NFSHelper::getattr(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, s = std::move(s)](size_t retryCounter) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<struct stat>(ECANCELED);

                struct nfs_stat_64 st;
                struct stat stbuf = {};
                auto error = nfs_stat64(m_nfs, fileId.c_str(), &st);

                if (error != 0) {
                    LOG_DBG(1) << "NFS getattr failed for " << fileId;

                    return one::helpers::makeFutureNFSException<struct stat>(
                        error, "getattr");
                }

                stbuf.st_dev = st.nfs_dev;
                stbuf.st_ino = st.nfs_ino;
                stbuf.st_mode = st.nfs_mode;
                stbuf.st_nlink = st.nfs_nlink;
                stbuf.st_uid = st.nfs_uid;
                stbuf.st_gid = st.nfs_gid;
                stbuf.st_rdev = st.nfs_rdev;
                stbuf.st_size = st.nfs_size;
                stbuf.st_blksize = st.nfs_blksize;
                stbuf.st_blocks = st.nfs_blocks;
                /*
                 *    stbuf.st_atim = st.nfs_atime;
                 *    stbuf.st_mtim = st.nfs_mtime;
                 *    stbuf.st_ctim = st.nfs_ctime;
                 *
                 */

                return folly::makeFuture(std::move(stbuf));
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mask);

    return connect().thenValue([this, fileId, mask,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, mask, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_access(m_nfs, fileId.c_str(), mask);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS access failed on " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "access");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::fbvector<folly::fbstring>> NFSHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return connect().thenValue([this, fileId, offset, count,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, offset, count, s = std::move(s)](
                size_t retryCounter) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<
                        folly::fbvector<folly::fbstring>>(ECANCELED);

                struct nfsdir *nfsdir{};
                struct nfsdirent *nfsdirent{};
                folly::fbvector<folly::fbstring> result;

                int ret = nfs_opendir(m_nfs, fileId.c_str(), &nfsdir);
                if (ret != 0) {
                    LOG(ERROR)
                        << "NFS failed to opendir(): " << nfs_get_error(nfs());

                    return one::helpers::makeFutureNFSException<
                        folly::fbvector<folly::fbstring>>(ret, "readdir");
                }

                int offset_ = offset;
                int count_ = count;
                while ((nfsdirent = nfs_readdir(m_nfs, nfsdir)) != NULL) {
                    if ((strcmp(nfsdirent->name, ".") != 0) &&
                        (strcmp(nfsdirent->name, "..") != 0)) {
                        if (offset_ > 0) {
                            --offset_;
                        }
                        else {
                            result.push_back(folly::fbstring(nfsdirent->name));
                            --count_;
                        }
                    }
                }

                nfs_closedir(m_nfs, nfsdir);

                LOG_DBG(2) << "Read directory " << fileId << " with entries "
                           << LOG_VEC(result);

                return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                    std::move(result));
            })
            .via(executor().get());
    });
}

folly::Future<folly::fbstring> NFSHelper::readlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<folly::fbstring>(ECANCELED);

                constexpr size_t kNFSReadlinkBufferSize = 4096U;
                auto buf = folly::IOBuf::create(kNFSReadlinkBufferSize);

                auto ret = nfs_readlink(m_nfs, fileId.c_str(),
                    reinterpret_cast<char *>(buf->writableData()),
                    kNFSReadlinkBufferSize - 1);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS readlink failed for " << fileId;

                    return one::helpers::makeFutureNFSException<
                        folly::fbstring>(ret, "readlink");
                }

                buf->append(ret);
                auto target = folly::fbstring{buf->moveToFbString().c_str()};

                LOG_DBG(2) << "Link " << fileId
                           << " read successfully - resolves to " << target;

                return folly::makeFuture(std::move(target));
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::mknod(const folly::fbstring &fileId,
    const mode_t unmaskedMode, const FlagsSet &flags, const dev_t rdev)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(unmaskedMode)
                << LOG_FARG(flagsToMask(flags));

    const mode_t mode = unmaskedMode | flagsToMask(flags) | S_IFREG;

    return connect().thenValue([this, fileId, mode, rdev,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, mode, rdev, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_mknod(m_nfs, fileId.c_str(), mode, rdev);

                if (ret != 0) {
                    LOG(ERROR) << "NFS mknod failed on " << fileId
                               << " with mode " << std::oct << mode << std::dec;

                    LOG(ERROR)
                        << "NFS failed to mknod(): " << nfs_get_error(nfs());

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "mknod");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return connect().thenValue([this, fileId, mode,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, mode, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_mkdir2(nfs(), fileId.c_str(), mode);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS mkdir failed for " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "mkdir");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::unlink(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_unlink(nfs(), fileId.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS unlink failed for " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "unlink");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_rmdir(nfs(), fileId.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS rmdir failed for " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "rmdir");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().thenValue([this, from, to,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, from, to, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_symlink(nfs(), from.c_str(), to.c_str());

                if (ret != 0) {
                    LOG_DBG(1)
                        << "NFS symlink failed for " << from << " to " << to;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "symlink");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().thenValue([this, from, to,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, from, to, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_rename(nfs(), from.c_str(), to.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS rename failed " << from << " to " << to;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "rename");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().thenValue([this, from, to,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, from, to, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_link(nfs(), from.c_str(), to.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS link failed " << from << " to " << to;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "link");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return connect().thenValue([this, fileId, mode,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, mode, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_chmod(nfs(), fileId.c_str(), mode);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS chmod failed for " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "chmod");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    return connect().thenValue([this, fileId, uid, gid,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [this, fileId, uid, gid, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_chown(nfs(), fileId.c_str(), uid, gid);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS chown failed for " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "chown");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    return connect().thenValue([this, fileId, size,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto && /*unit*/) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [nfs = m_nfs, fileId, size, s = std::move(s)](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<folly::Unit>(ECANCELED);

                auto ret = nfs_truncate(nfs, fileId.c_str(), size);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS truncate failed on " << fileId;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "truncate");
                }

                return folly::makeFuture();
            })
            .via(executor().get());
    });
}

folly::Future<folly::Unit> NFSHelper::connect()
{
    LOG_FCALL();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [this, s = std::weak_ptr<NFSHelper>{shared_from_this()}](size_t n) {
            auto self = s.lock();
            if (!self)
                return makeFutureNFSException(ECANCELED, "connect");

            if (m_isConnected)
                return folly::makeFuture();

            LOG(ERROR) << "Attempting (" << n
                       << ") to connect to NFS server at: " << host()
                       << " path: " << volume();

            m_nfs = nfs_init_context();
            if (m_nfs == nullptr) {
                LOG(ERROR) << "Failed to init NFS context";
                return makeFutureNFSException(EIO, "init");
            }

            nfs_set_version(m_nfs, version());
            nfs_set_debug(m_nfs, 9);
            nfs_set_uid(m_nfs, uid());
            nfs_set_gid(m_nfs, gid());

            LOG(ERROR) << "Calling NFS mount";

            auto ret = nfs_mount(m_nfs, host().c_str(), volume().c_str());

            if (ret != 0) {
                LOG(ERROR) << "NFS mount failed";
                return makeFutureNFSException(ret, "mount");
            }

            LOG(ERROR) << "NFS mount succeeded";

            return folly::makeFuture();
        })
        .via(executor().get());
}

NFSHelperFactory::NFSHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
    : m_executor{std::move(executor)}
{
}

folly::fbstring NFSHelperFactory::name() const { return NFS_HELPER_NAME; }

std::vector<folly::fbstring> NFSHelperFactory::overridableParams() const
{
    return {"host", "volume", "traverseMounts", "readahead", "tcpSyncnt",
        "dirCache", "autoreconnect"};
}

std::shared_ptr<StorageHelper> NFSHelperFactory::createStorageHelper(
    const Params &parameters, ExecutionContext executionContext)
{
    auto params = NFSHelperParams::create(parameters);
    if ((params->version()) == 3 || (params->version() == 4)) {
        return std::make_shared<NFSHelper>(std::move(params), m_executor,
            constants::ASYNC_OPS_TIMEOUT, executionContext);
    }

    throw std::invalid_argument(
        "NFS driver currently support only NFS version 3 and 4.");
}

} // namespace helpers
} // namespace one
