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

namespace {
void readCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = static_cast<folly::Promise<folly::IOBufQueue> *>(pdata);

    if (status < 0) {
        promise->setException(makeNFSException(status, "read"));
        return;
    }

    folly::IOBufQueue bufQueue{folly::IOBufQueue::cacheChainLength()};
    bufQueue.append(folly::IOBuf::takeOwnership(data, status, status));

    promise->setValue(std::move(bufQueue));
}
}

folly::Future<folly::IOBufQueue> NFSFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto promise = std::make_shared<folly::Promise<folly::IOBufQueue>>();

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh, promise, offset, size](
            size_t retryCounter) {
            auto future = promise->getFuture();
            auto error = nfs_pread_async(
                nfs, nfsFh, offset, size, readCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs read on " << fileId;

                return one::helpers::makeFutureNFSException<folly::IOBufQueue>(
                    error, "read");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return std::move(res); });
}

namespace {
void writeCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = static_cast<folly::Promise<size_t> *>(pdata);

    if (status < 0) {
        promise->setException(makeNFSException(status, "write"));
        return;
    }

    promise->setValue(status);
}
}

folly::Future<std::size_t> NFSFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset);

    if (buf.empty())
        return folly::makeFuture<std::size_t>(0);

    auto promise = std::make_shared<folly::Promise<std::size_t>>();

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    const auto size = buf.chainLength();

    auto iobuf = buf.move();
    if (iobuf->isChained()) {
        LOG_DBG(2) << "Coalescing chained buffer at offset " << offset
                   << " of size: " << iobuf->length();
        iobuf->unshare();
        iobuf->coalesce();
    }

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh, promise,
            data = iobuf->writableData(), offset, size](size_t retryCounter) {
            auto future = promise->getFuture();

            auto error = nfs_pwrite_async(
                nfs, nfsFh, offset, size, data, writeCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs write on " << fileId;

                return one::helpers::makeFutureNFSException<std::size_t>(
                    error, "write");
            }

            return future;
        })
        .via(executor().get())
        .then([promise, buf = std::move(iobuf), writeCb = std::move(writeCb)](
                  folly::Try<std::size_t> &&res) {
            if (res.hasValue() && writeCb)
                writeCb(res.value());
            return res;
        });
}

namespace {
void releaseCallback(
    int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = static_cast<folly::Promise<folly::Unit> *>(pdata);

    if (status < 0) {
        promise->setException(makeNFSException(status, "release"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSFileHandle::release()
{
    LOG_FCALL();

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh, promise](
            size_t retryCounter) {
            auto future = promise->getFuture();

            auto error =
                nfs_close_async(nfs, nfsFh, releaseCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs close on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "release");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

folly::Future<folly::Unit> NFSFileHandle::flush() { return {}; }

namespace {
void fsyncCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = static_cast<folly::Promise<folly::Unit> *>(pdata);

    if (status < 0) {
        promise->setException(makeNFSException(status, "fsync"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSFileHandle::fsync(bool /*isDataSync*/)
{
    LOG_FCALL();

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    auto nfs = std::dynamic_pointer_cast<NFSHelper>(helper())->nfs();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [fileId = fileId(), nfs, nfsFh = m_nfsFh, promise](
            size_t retryCounter) {
            auto future = promise->getFuture();

            auto error =
                nfs_fsync_async(nfs, nfsFh, fsyncCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs fsync on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "fsync");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
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

namespace {
struct NFSOpenCallbackData {
    std::shared_ptr<folly::Promise<std::shared_ptr<NFSFileHandle>>> promise{};
    std::shared_ptr<NFSHelper> helper{};
    folly::fbstring fileId{};
};

void openCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *cbData = static_cast<NFSOpenCallbackData *>(pdata);
    auto *nfsFh = static_cast<struct nfsfh *>(data);

    if (status < 0) {
        cbData->promise->setException(makeNFSException(status, "open"));
        return;
    }

    auto executor = cbData->helper->executor();
    auto timeout = cbData->helper->timeout();
    auto handle = std::make_shared<NFSFileHandle>(cbData->fileId,
        std::move(cbData->helper), nfsFh, std::move(executor), timeout);

    cbData->promise->setValue(std::move(handle));
}
}

folly::Future<FileHandlePtr> NFSHelper::open(const folly::fbstring &fileId,
    const int flags, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(flags);

    auto cbData = std::make_shared<NFSOpenCallbackData>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, cbData, fileId, flags](size_t retryCounter) {
            auto future = cbData->promise->getFuture();
            auto error = nfs_open_async(
                nfs, fileId.c_str(), flags, openCallback, cbData.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs open on " << fileId;

                return one::helpers::makeFutureNFSException<
                    std::shared_ptr<NFSFileHandle>>(error, "open");
            }

            return future;
        })
        .via(executor().get())
        .then([cbData](auto &&res) { return res; });
}

namespace {
void getattrCallback(
    int status, struct nfs_context *nfs, void *statbuf, void *data)
{
    auto *promise = (folly::Promise<struct stat> *)data;
    auto *st = (struct nfs_stat_64 *)statbuf;
    struct stat stbuf = {};

    if (status < 0) {
        promise->setException(makePosixException(status));
    }

    stbuf.st_dev = st->nfs_dev;
    stbuf.st_ino = st->nfs_ino;
    stbuf.st_mode = st->nfs_mode;
    stbuf.st_nlink = st->nfs_nlink;
    stbuf.st_uid = st->nfs_uid;
    stbuf.st_gid = st->nfs_gid;
    stbuf.st_rdev = st->nfs_rdev;
    stbuf.st_size = st->nfs_size;
    stbuf.st_blksize = st->nfs_blksize;
    stbuf.st_blocks = st->nfs_blocks;
    /*
     *    stbuf.st_atim = st->nfs_atime;
     *    stbuf.st_mtim = st->nfs_mtime;
     *    stbuf.st_ctim = st->nfs_ctime;
     *
     */
    promise->setValue(std::move(stbuf));
}
}

folly::Future<struct stat> NFSHelper::getattr(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto promise = std::make_shared<folly::Promise<struct stat>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId](size_t retryCounter) {
            auto future = promise->getFuture();
            auto error = nfs_stat64_async(
                nfs, fileId.c_str(), getattrCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs stat on " << fileId;

                return one::helpers::makeFutureNFSException<struct stat>(
                    error, "getattr");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void accessCallback(
    int status, struct nfs_context *nfs, void *statbuf, void *data)
{
    auto *promise = (folly::Promise<folly::Unit> *)data;

    if (status < 0) {
        promise->setException(makeNFSException(status, "access"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mask);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId, mask](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_access_async(
                nfs, fileId.c_str(), mask, accessCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs access on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "access");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

folly::Future<folly::fbvector<folly::fbstring>> NFSHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, fileId, offset, count](size_t retryCounter) {
            struct nfsdir *nfsdir{};
            struct nfsdirent *nfsdirent{};
            folly::fbvector<folly::fbstring> result;

            if (nfs_opendir(nfs, fileId.c_str(), &nfsdir)) {
                LOG(ERROR) << "NFS failed to opendir(): " << nfs_get_error(nfs);

                return one::helpers::makeFutureNFSException<
                    folly::fbvector<folly::fbstring>>(EIO, "readdir");
            }

            int offset_ = offset;
            int count_ = count;
            while ((nfsdirent = nfs_readdir(nfs, nfsdir)) != NULL) {
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

            nfs_closedir(nfs, nfsdir);

            LOG_DBG(2) << "Read directory " << fileId << " with entries "
                       << LOG_VEC(result);

            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(result));
        })
        .via(executor().get());
}

namespace {
constexpr size_t kNFSReadlinkBufferSize = 4096U;

void readlinkCallback(
    int status, struct nfs_context *nfs, void *link, void *data)
{
    auto *promise = (folly::Promise<folly::fbstring> *)data;

    if (status < 0) {
        promise->setException(makeNFSException(status, "readlink"));
        return;
    }

    if (strlen(static_cast<char *>(link)) > kNFSReadlinkBufferSize) {
        promise->setException(makeNFSException(ENAMETOOLONG, "readlink"));
        return;
    }

    folly::fbstring result{static_cast<char *>(link)};

    promise->setValue(std::move(result));
}
}

folly::Future<folly::fbstring> NFSHelper::readlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto promise = std::make_shared<folly::Promise<folly::fbstring>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_readlink_async(
                nfs, fileId.c_str(), readlinkCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs readlink on " << fileId;

                return one::helpers::makeFutureNFSException<folly::fbstring>(
                    error, "readlink");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void mknodCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "mknod"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::mknod(const folly::fbstring &fileId,
    const mode_t unmaskedMode, const FlagsSet &flags, const dev_t rdev)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(unmaskedMode)
                << LOG_FARG(flagsToMask(flags));

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    const mode_t mode = unmaskedMode | flagsToMask(flags);

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId, mode, rdev](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_mknod_async(
                nfs, fileId.c_str(), mode, rdev, mknodCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs mknod on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "mknod");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void mkdirCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "mkdir"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId, mode](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_mkdir2_async(
                nfs, fileId.c_str(), mode, mkdirCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs mkdir on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "mkdir");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void unlinkCallback(
    int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "unlink"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::unlink(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_unlink_async(
                nfs, fileId.c_str(), unlinkCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs unlink on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "unlink");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void rmdirCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "rmdir"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_rmdir_async(
                nfs, fileId.c_str(), rmdirCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs rmdir on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "rmdir");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void symlinkCallback(
    int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "symlink"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, from, to](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_symlink_async(
                nfs, from.c_str(), to.c_str(), symlinkCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs symlink from " << from
                           << " to " << to;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "symlink");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void renameCallback(
    int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "rename"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, from, to](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_rename_async(
                nfs, from.c_str(), to.c_str(), renameCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs rename from " << from
                           << " to " << to;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "rename");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void linkCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "link"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, from, to](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_link_async(
                nfs, from.c_str(), to.c_str(), linkCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs link from " << from
                           << " to " << to;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "link");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void chmodCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "chmod"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId, mode](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_chmod_async(
                nfs, fileId.c_str(), mode, chmodCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs chmod on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "chmod");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void chownCallback(int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "chown"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId, uid, gid](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_chown_async(
                nfs, fileId.c_str(), uid, gid, chownCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs chown on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "chown");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void truncateCallback(
    int status, struct nfs_context *nfs, void *data, void *pdata)
{
    auto *promise = (folly::Promise<folly::Unit> *)pdata;

    if (status < 0) {
        promise->setException(makeNFSException(status, "truncate"));
        return;
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [nfs = m_nfs, promise, fileId, size](size_t retryCount) {
            auto future = promise->getFuture();
            auto error = nfs_truncate_async(
                nfs, fileId.c_str(), size, truncateCallback, promise.get());

            if (error != 0) {
                LOG_DBG(1) << "Failed to call async nfs truncate on " << fileId;

                return one::helpers::makeFutureNFSException<folly::Unit>(
                    error, "truncate");
            }

            return future;
        })
        .via(executor().get())
        .then([promise](auto &&res) { return res; });
}

namespace {
void mountCallback(
    int status, struct nfs_context *nfs, void *statbuf, void *data)
{
    auto *promise = (folly::Promise<folly::Unit> *)data;

    if (status < 0) {
        promise->setException(makeNFSException(status, "mount"));
    }

    promise->setValue();
}
}

folly::Future<folly::Unit> NFSHelper::connect()
{
    LOG_FCALL();

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();

    return folly::futures::retrying(NFSRetryPolicy(constants::IO_RETRY_COUNT),
        [this, promise, s = std::weak_ptr<NFSHelper>{shared_from_this()}](
            size_t n) {
            auto self = s.lock();
            if (!self)
                return makeFutureNFSException(ECANCELED, "connect");

            if (m_isConnected)
                return folly::makeFuture();

            LOG_DBG(1) << "Attempting to connect to NFS server at: " << host()
                       << " path: " << path();

            m_nfs = nfs_init_context();
            if (m_nfs == nullptr) {
                LOG(ERROR) << "Failed to init NFS context";
                return makeFutureNFSException(EIO, "init");
            }

            auto future = promise->getFuture();

            auto ret = nfs_mount_async(m_nfs, host().c_str(), path().c_str(),
                mountCallback, promise.get());

            if (ret != 0) {
                LOG(ERROR) << "Failed to start async nfs mount";
                return makeFutureNFSException(EIO, "mount");
            }

            return future;
        })
        .via(executor().get())
        .then([this, promise](auto &&res) {
            if (res.hasValue())
                m_isConnected = true;
            return res;
        });
}

NFSHelperFactory::NFSHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
    : m_executor{std::move(executor)}
{
}

folly::fbstring NFSHelperFactory::name() const { return NFS_HELPER_NAME; }

std::vector<folly::fbstring> NFSHelperFactory::overridableParams() const
{
    return {"host", "path", "traverseMounts", "readahead", "tcpSyncnt",
        "dirCache", "autoreconnect"};
}

std::shared_ptr<StorageHelper> NFSHelperFactory::createStorageHelper(
    const Params &parameters, ExecutionContext executionContext)
{
    auto params = NFSHelperParams::create(parameters);
    if (params->version() == 3 || params->version() == 4) {
        return std::make_shared<NFSHelper>(std::move(params), m_executor,
            constants::ASYNC_OPS_TIMEOUT, executionContext);
    }

    throw std::invalid_argument(
        "NFS driver currently support only NFS version 3 and 4.");
}

} // namespace helpers
} // namespace one
