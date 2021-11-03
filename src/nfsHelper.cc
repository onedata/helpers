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

#include <random>

namespace one {
namespace helpers {

namespace {
const std::set<int> &NFSRetryErrors()
{
    static const std::set<int> NFS_RETRY_ERRORS = {EINTR, EIO, EAGAIN, EACCES,
        EBUSY, EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK, EWOULDBLOCK,
        ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN, ENETUNREACH, ECONNABORTED,
        ECONNRESET, ENOTCONN, EHOSTUNREACH, ECANCELED};
    return NFS_RETRY_ERRORS;
}

class nfs_system_error : public std::system_error {
public:
    nfs_system_error(std::error_code ec, std::string op)
        : std::system_error{ec}
        , operation{std::move(op)}
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

inline auto NFSRetryPolicy(size_t maxTries)
{
    constexpr auto k_retryBackoffMinMs = 250;
    constexpr auto k_retryBackoffMaxMs = 2000;
    constexpr auto k_retryJitter = 0.5;

    return folly::futures::retryingPolicyCappedJitteredExponentialBackoff(
        maxTries, std::chrono::milliseconds{k_retryBackoffMinMs},
        std::chrono::milliseconds{k_retryBackoffMaxMs}, k_retryJitter,
        std::mt19937_64(std::random_device{}()),
        [maxTries](size_t n, const folly::exception_wrapper &ew) {
            const auto *e = ew.get_exception<nfs_system_error>();
            if (e == nullptr)
                return false;

            auto shouldRetry = (n < maxTries) &&
                ((e->code().value() >= 0) &&
                    (NFSRetryErrors().find(e->code().value()) !=
                        NFSRetryErrors().end()));

            if (shouldRetry) {
                LOG_DBG(1) << "Retrying NFS helper operation '" << e->operation
                           << "' due to error: " << e->code();
                ONE_METRIC_COUNTER_INC(
                    "comp.helpers.mod.nfs." + e->operation + ".retries");
            }

            return shouldRetry;
        });
}
} // namespace

NFSFileHandle::NFSFileHandle(const folly::fbstring &fileId,
    std::shared_ptr<NFSHelper> helper, struct nfsfh *nfsFh,
    std::shared_ptr<folly::Executor> executor, Timeout timeout)
    : FileHandle{fileId, std::move(helper)}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
    , m_nfsFh{nfsFh}
{
}

folly::Future<folly::IOBufQueue> NFSFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.nfs.read");

    auto *helperPtr = std::dynamic_pointer_cast<NFSHelper>(helper()).get();

    auto readOp = [this, fileId = fileId(), offset, size,
                      timer = std::move(timer),
                      s = std::weak_ptr<NFSFileHandle>{shared_from_this()}](
                      NFSConnection *conn, size_t retryCount) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::IOBufQueue>(ECANCELED);

        auto *nfs = conn->nfs;
        const size_t maxBlock = conn->maxReadSize;

        folly::IOBufQueue buffer{folly::IOBufQueue::cacheChainLength()};
        char *raw = static_cast<char *>(buffer.preallocate(size, size).first);

        LOG_DBG(2) << "Attempting to read " << size << " bytes at offset "
                   << offset << " from file " << fileId;

        int ret = 0;
        size_t bufOffset = 0;

        while (bufOffset < size) {
            ret = nfs_pread(nfs, m_nfsFh,
                offset + bufOffset,                   // NOLINT
                std::min(maxBlock, size - bufOffset), // NOLINT
                raw + bufOffset);                     // NOLINT

            if (ret == 0)
                break;

            if (ret < 0) {
                LOG_DBG(1) << "NFS read failed from " << fileId
                           << " with error: " << nfs_get_error(nfs)
                           << " - retry " << retryCount;

                return one::helpers::makeFutureNFSException<folly::IOBufQueue>(
                    ret, "read");
            }

            bufOffset += ret;
        }

        buffer.postallocate(bufOffset);

        LOG_DBG(2) << "Read " << bufOffset << " from file " << fileId;

        ONE_METRIC_TIMERCTX_STOP(timer, bufOffset);

        return folly::makeFuture(std::move(buffer));
    };

    return helperPtr->connect().thenValue(
        [this, helperPtr, readOp = std::move(readOp)](auto &&conn) {
            return folly::futures::retrying(
                NFSRetryPolicy(constants::IO_RETRY_COUNT),
                [conn, readOp = std::move(readOp)](
                    int retryCount) { return readOp(conn, retryCount); })
                .via(executor().get())
                .thenTry([helperPtr, conn](auto &&v) {
                    helperPtr->putBackConnection(conn);
                    return std::forward<decltype(v)>(v);
                });
        });
}

folly::Future<std::size_t> NFSFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.nfs.write");

    if (buf.empty()) {
        ONE_METRIC_TIMERCTX_STOP(timer, 0);
        return folly::makeFuture<std::size_t>(0);
    }

    auto *helperPtr = std::dynamic_pointer_cast<NFSHelper>(helper()).get();

    auto writeOp = [this, fileId = fileId(), offset, buf = std::move(buf),
                       timer = std::move(timer), writeCb = std::move(writeCb),
                       s = std::weak_ptr<NFSFileHandle>{shared_from_this()}](
                       NFSConnection *conn, size_t retryCount) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<std::size_t>(ECANCELED);

        auto *nfs = conn->nfs;
        const size_t maxBlock = conn->maxWriteSize;

        const auto size = buf.chainLength();

        auto iobuf = buf.move();
        if (iobuf->isChained()) {
            LOG_DBG(2) << "Coalescing chained buffer at offset " << offset
                       << " of size: " << iobuf->length();
            iobuf->unshare();
            iobuf->coalesce();
        }

        int ret = 0;
        std::size_t bufOffset = 0;
        while (bufOffset < size) {
            ret = nfs_pwrite(nfs, m_nfsFh, offset + bufOffset, // NOLINT
                std::min(maxBlock, size - bufOffset),          // NOLINT
                iobuf->writableData() + bufOffset);            // NOLINT

            if (ret < 0) {
                LOG_DBG(1) << "NFS write failed for " << fileId << " : "
                           << nfs_get_error(nfs) << " - retry " << retryCount;

                return one::helpers::makeFutureNFSException<std::size_t>(
                    ret, "write");
            }

            bufOffset += ret;
        }

        if (writeCb)
            writeCb(bufOffset);

        LOG_DBG(2) << "Written " << bufOffset << " bytes to file " << fileId;

        ONE_METRIC_TIMERCTX_STOP(timer, bufOffset);

        return folly::makeFuture(bufOffset);
    };

    return helperPtr->connect().thenValue(
        [this, helperPtr, writeOp = std::move(writeOp)](auto &&conn) mutable {
            return folly::futures::retrying(
                NFSRetryPolicy(constants::IO_RETRY_COUNT),
                [conn, writeOp = std::move(writeOp)](int retryCount) mutable {
                    return writeOp(conn, retryCount);
                })
                .via(executor().get())
                .thenTry([helperPtr, conn](auto &&v) {
                    helperPtr->putBackConnection(conn);
                    return std::forward<decltype(v)>(v);
                });
        });
}

folly::Future<folly::Unit> NFSFileHandle::release()
{
    LOG_FCALL();

    auto *helperPtr = std::dynamic_pointer_cast<NFSHelper>(helper()).get();

    auto releaseOp = [this, fileId = fileId(),
                         s = std::weak_ptr<NFSFileHandle>{shared_from_this()}](
                         NFSConnection *conn, size_t retryCount) {
        auto self = s.lock();
        if (!self)
            return folly::makeFuture();

        auto *nfs = conn->nfs;
        auto ret = nfs_close(nfs, m_nfsFh);

        if (ret != 0) {
            LOG_DBG(1) << "Failed to release file " << fileId << " due to "
                       << nfs_get_error(nfs) << " - retry " << retryCount;

            return one::helpers::makeFutureNFSException<folly::Unit>(
                ret, "release");
        }

        return folly::makeFuture();
    };

    return helperPtr->connect().thenValue(
        [this, helperPtr, releaseOp = std::move(releaseOp)](auto &&conn) {
            return folly::futures::retrying(
                NFSRetryPolicy(constants::IO_RETRY_COUNT),
                [conn, releaseOp = std::move(releaseOp)](
                    int retryCount) { return releaseOp(conn, retryCount); })
                .via(executor().get())
                .thenTry([helperPtr, conn](auto &&v) {
                    helperPtr->putBackConnection(conn);
                    return std::forward<decltype(v)>(v);
                });
        });
}

folly::Future<folly::Unit> NFSFileHandle::flush() { return {}; }

folly::Future<folly::Unit> NFSFileHandle::fsync(bool /*isDataSync*/)
{
    LOG_FCALL();

    auto *helperPtr = std::dynamic_pointer_cast<NFSHelper>(helper()).get();

    auto fsyncOp = [this, fileId = fileId(),
                       s = std::weak_ptr<NFSFileHandle>{shared_from_this()}](
                       NFSConnection *conn, size_t retryCount) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException(ECANCELED);

        auto *nfs = conn->nfs;

        auto ret = nfs_fsync(nfs, m_nfsFh);

        if (ret != 0) {
            LOG_DBG(1) << "Failed to fsync file " << fileId << " - retry "
                       << retryCount;

            return one::helpers::makeFutureNFSException<folly::Unit>(
                ret, "fsync");
        }

        return folly::makeFuture();
    };

    return helperPtr->connect().thenValue(
        [this, helperPtr, fsyncOp = std::move(fsyncOp)](auto &&conn) {
            return folly::futures::retrying(
                NFSRetryPolicy(constants::IO_RETRY_COUNT),
                [conn, fsyncOp = std::move(fsyncOp)](
                    int retryCount) { return fsyncOp(conn, retryCount); })
                .via(executor().get())
                .thenTry([helperPtr, conn](auto &&v) {
                    helperPtr->putBackConnection(conn);
                    return std::forward<decltype(v)>(v);
                });
        });
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

void NFSHelper::putBackConnection(NFSConnection *conn)
{
    m_idleConnections.emplace(conn);
}

folly::Future<FileHandlePtr> NFSHelper::open(const folly::fbstring &fileId,
    const int flags, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(flags);
    auto openFunc = [fileId, helper = shared_from_this(), executor = executor(),
                        timeout = timeout()](NFSConnection *conn, auto &&mode) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [conn, fileId, mode, helper, executor, timeout](size_t retryCount) {
                struct nfsfh *nfsFh{nullptr};

                LOG_DBG(1) << "Attempting to open file " << fileId
                           << " - retry " << retryCount;

                auto ret = nfs_open(conn->nfs, fileId.c_str(), mode, &nfsFh);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS open failed on " << fileId << " with "
                               << nfs_get_error(conn->nfs) << " - retry "
                               << retryCount;

                    return one::helpers::makeFutureNFSException<
                        std::shared_ptr<NFSFileHandle>>(ret, "open");
                }

                return folly::makeFuture<std::shared_ptr<NFSFileHandle>>(
                    std::make_shared<NFSFileHandle>(
                        fileId, helper, nfsFh, executor, timeout));
            });
    };

    if (version() == 3 || (flags & O_CREAT) == 0) {
        return connect().thenValue(
            [this, flags, openFunc = std::move(openFunc)](auto &&conn) {
                return openFunc(conn, flags)
                    .via(executor().get())
                    .thenTry([this, conn](auto &&v) {
                        putBackConnection(conn);
                        return std::forward<decltype(v)>(v);
                    });
            });
    }

    return access(fileId, flags)
        // If the file exists, remove the O_CREAT flag
        .thenValue([flags](auto && /*unit*/) { return flags & ~O_CREAT; })
        .thenError(folly::tag_t<std::system_error>{},
            [flags](auto && /*unit*/) { return flags; })
        .thenValue([this, openFunc = std::move(openFunc)](auto &&mode) {
            return connect().thenValue(
                [this, mode, openFunc = std::move(openFunc)](auto &&conn) {
                    return openFunc(conn, mode)
                        .via(executor().get())
                        .thenTry([this, conn](auto &&v) {
                            putBackConnection(conn);
                            return std::forward<decltype(v)>(v);
                        });
                });
        });
}

folly::Future<struct stat> NFSHelper::getattr(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<struct stat>(ECANCELED);

                struct nfs_stat_64 st {
                };
                struct stat stbuf = {};
                auto error = nfs_stat64(conn->nfs, fileId.c_str(), &st);

                if (error != 0) {
                    LOG_DBG(1) << "NFS getattr failed for " << fileId
                               << " - retry " << retryCount;

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
                stbuf.st_atim.tv_sec = st.nfs_atime;
                stbuf.st_atim.tv_nsec = st.nfs_atime_nsec;
                stbuf.st_mtim.tv_sec = st.nfs_mtime;
                stbuf.st_mtim.tv_nsec = st.nfs_mtime_nsec;
                stbuf.st_ctim.tv_sec = st.nfs_ctime;
                stbuf.st_ctim.tv_nsec = st.nfs_ctime_nsec;

                return folly::makeFuture(stbuf);
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mask);

    return connect().thenValue([this, fileId, mask,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [conn, fileId, mask, s](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_access(conn->nfs, fileId.c_str(), mask);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS access failed on " << fileId
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "access");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::fbvector<folly::fbstring>> NFSHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return connect().thenValue([this, fileId, offset, count,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, offset, count, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<
                        folly::fbvector<folly::fbstring>>(ECANCELED);

                struct nfsdir *nfsdir{};
                struct nfsdirent *nfsdirent{};
                folly::fbvector<folly::fbstring> result;

                int ret = nfs_opendir(conn->nfs, fileId.c_str(), &nfsdir);
                if (ret != 0) {
                    LOG_DBG(1) << "NFS failed to opendir(): "
                               << nfs_get_error(conn->nfs) << " - retry "
                               << retryCount;

                    return one::helpers::makeFutureNFSException<
                        folly::fbvector<folly::fbstring>>(ret, "readdir");
                }

                int offset_ = offset;
                int count_ = count;
                while (
                    (nfsdirent = nfs_readdir(conn->nfs, nfsdir)) != nullptr) {
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

                nfs_closedir(conn->nfs, nfsdir);

                LOG_DBG(2) << "Read directory " << fileId << " with entries "
                           << LOG_VEC(result);

                return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                    std::move(result));
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::fbstring> NFSHelper::readlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<folly::fbstring>(ECANCELED);

                char *buf{nullptr};
                auto ret = nfs_readlink2(conn->nfs, fileId.c_str(), &buf);

                if ((ret != 0) || (buf == nullptr)) {
                    LOG_DBG(1) << "NFS readlink failed for " << fileId
                               << " - retry " << retryCount;

                    if (buf != nullptr)
                        free(buf); // NOLINT

                    return one::helpers::makeFutureNFSException<
                        folly::fbstring>(ret, "readlink");
                }

                folly::fbstring target{buf};
                free(buf); // NOLINT

                LOG_DBG(2) << "Link " << fileId
                           << " read successfully - resolves to " << target;

                return folly::makeFuture(std::move(target));
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::mknod(const folly::fbstring &fileId,
    const mode_t unmaskedMode, const FlagsSet &flags, const dev_t /*rdev*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(unmaskedMode)
                << LOG_FARG(flagsToMask(flags));

    const mode_t mode = unmaskedMode | flagsToMask(flags);

    return access(fileId, mode)
        .thenError(folly::tag_t<std::system_error>{},
            [this, fileId, mode,
                s = std::weak_ptr<NFSHelper>{shared_from_this()}](auto &&e) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<folly::Unit>(ECANCELED);

                if (e.code().value() == ENOENT) {
                    return open(fileId, static_cast<int>(mode | O_CREAT), {})
                        .thenTry([](auto &&maybeHandle) {
                            if (maybeHandle.hasException()) {
                                LOG_DBG(1) << "Creating failed - exception: "
                                           << maybeHandle.exception().what();
                                maybeHandle.exception().throw_exception();
                            }

                            return maybeHandle.value()->release();
                        });
                }

                throw e;
            })
        .thenValue([](auto && /*unit*/) { return folly::makeFuture(); });
}

folly::Future<folly::Unit> NFSHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return connect().thenValue([this, fileId, mode,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, mode, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_mkdir2(conn->nfs, fileId.c_str(), mode);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS mkdir failed for " << fileId
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "mkdir");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::unlink(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_unlink(conn->nfs, fileId.c_str());

                if ((ret != 0) && (ret != -ESTALE)) {
                    LOG_DBG(1) << "NFS unlink failed for " << fileId
                               << " due to " << nfs_get_error(conn->nfs)
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "unlink");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return connect().thenValue([this, fileId,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_rmdir(conn->nfs, fileId.c_str());

                if ((ret != 0) && (ret != -ESTALE)) {
                    LOG_DBG(1) << "NFS rmdir failed for " << fileId
                               << " due to " << nfs_get_error(conn->nfs)
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "rmdir");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().thenValue([this, from, to,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [from, to, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_symlink(conn->nfs, from.c_str(), to.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS symlink failed for " << from << " to "
                               << to << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "symlink");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().thenValue([this, from, to,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [from, to, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_rename(conn->nfs, from.c_str(), to.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS rename failed " << from << " to " << to
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "rename");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return connect().thenValue([this, from, to,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [from, to, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_link(conn->nfs, from.c_str(), to.c_str());

                if (ret != 0) {
                    LOG_DBG(1) << "NFS link failed " << from << " to " << to
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "link");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return connect().thenValue([this, fileId, mode,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, mode, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_chmod(conn->nfs, fileId.c_str(), mode);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS chmod failed for " << fileId
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "chmod");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    return connect().thenValue([this, fileId, uid, gid,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, uid, gid, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException(ECANCELED);

                auto ret = nfs_chown(conn->nfs, fileId.c_str(), uid, gid);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS chown failed for " << fileId
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "chown");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<folly::Unit> NFSHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    return connect().thenValue([this, fileId, size,
                                   s = std::weak_ptr<NFSHelper>{
                                       shared_from_this()}](auto &&conn) {
        return folly::futures::retrying(
            NFSRetryPolicy(constants::IO_RETRY_COUNT),
            [fileId, size, s, conn](size_t retryCount) {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<folly::Unit>(ECANCELED);

                auto ret = nfs_truncate(conn->nfs, fileId.c_str(), size);

                if (ret != 0) {
                    LOG_DBG(1) << "NFS truncate failed on " << fileId
                               << " due to " << nfs_get_error(conn->nfs)
                               << " - retry " << retryCount;

                    return one::helpers::makeFutureNFSException<folly::Unit>(
                        ret, "truncate");
                }

                return folly::makeFuture();
            })
            .via(executor().get())
            .thenTry([this, conn](auto &&v) {
                putBackConnection(conn);
                return std::forward<decltype(v)>(v);
            });
    });
}

folly::Future<NFSConnection *> NFSHelper::connect()
{
    LOG_FCALL();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [this, s = std::weak_ptr<NFSHelper>{shared_from_this()}](size_t n) {
            auto self = s.lock();
            if (!self)
                return makeFutureNFSException<NFSConnection *>(
                    ECANCELED, "connect");

            std::call_once(m_connectionFlag, [&]() {
                LOG_DBG(2) << "Attempting (" << n
                           << ") to connect to NFS server at: " << host()
                           << " path: " << volume();

                constexpr auto k_connectionPoolDefaultSize{10UL};
                size_t connectionPoolSize{k_connectionPoolDefaultSize};
                auto threadPool =
                    std::dynamic_pointer_cast<folly::ThreadPoolExecutor>(
                        executor());
                if (threadPool)
                    connectionPoolSize = threadPool->numThreads();

                for (unsigned int i = 0; i < connectionPoolSize; i++) {
                    auto conn = std::make_unique<NFSConnection>();

                    conn->nfs = nfs_init_context();
                    if (conn->nfs == nullptr) {
                        LOG(ERROR) << "Failed to init NFS context for host: "
                                   << host();
                        throw makeNFSException(EIO, "init");
                    }

                    nfs_set_version(conn->nfs, version());
                    nfs_set_uid(conn->nfs, uid());
                    nfs_set_gid(conn->nfs, gid());
                    nfs_set_timeout(conn->nfs, timeout().count());
                    nfs_set_dircache(conn->nfs, dircache() ? 1 : 0);
                    nfs_set_readahead(conn->nfs, readahead());
                    nfs_set_tcp_syncnt(conn->nfs, tcpSyncnt());
                    nfs_set_autoreconnect(conn->nfs, autoreconnect() ? 1 : 0);

                    LOG_DBG(2) << "Calling NFS mount";

                    auto ret =
                        nfs_mount(conn->nfs, host().c_str(), volume().c_str());

                    if (ret != 0) {
                        LOG(ERROR)
                            << "NFS mount failed: " << nfs_get_error(conn->nfs);
                        throw makeNFSException(ret, "mount");
                    }

                    conn->isConnected = true;

                    LOG_DBG(2) << "NFS mount succeeded";

                    const size_t kFallbackTransferSize = 2 * 1024;
                    const size_t kTransferSizeWarningThreshold = 1024 * 1024;

                    conn->maxReadSize = nfs_get_readmax(conn->nfs) != 0U
                        ? nfs_get_readmax(conn->nfs)
                        : kFallbackTransferSize;

                    conn->maxWriteSize = nfs_get_writemax(conn->nfs) != 0U
                        ? nfs_get_writemax(conn->nfs)
                        : kFallbackTransferSize;

                    if (conn->maxReadSize < kTransferSizeWarningThreshold)
                        LOG(WARNING) << "NFS server at " << host()
                                     << " has very low read transfer size: "
                                     << conn->maxReadSize;

                    if (conn->maxWriteSize < kTransferSizeWarningThreshold)
                        LOG(WARNING) << "NFS server at " << host()
                                     << " has very low write transfer size: "
                                     << conn->maxWriteSize;

                    m_idleConnections.emplace(conn.get());
                    m_connections.emplace_back(std::move(conn));
                }

                m_isConnected = true;
            });

            NFSConnection *c{nullptr};

            constexpr auto k_connectionPopDelayMs = 1000;
            if (!m_idleConnections.try_pop(c)) {
                LOG_DBG(2) << "Waiting for idle NFS connection...";
                return folly::via(executor().get())
                    .delayed(std::chrono::milliseconds{k_connectionPopDelayMs})
                    .thenValue([this](auto && /*unit*/) { return connect(); });
            }

            return folly::makeFuture(c);
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
    return {"version", "host", "volume", "traverseMounts", "readahead",
        "tcpSyncnt", "dircache", "autoreconnect"};
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
