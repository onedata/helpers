/**
 * @file posixHelper.cc
 * @author Rafal Slota
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif // linux

#include "posixHelper.h"
#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <boost/any.hpp>

#include <cerrno>
#include <dirent.h>
#if FUSE_USE_VERSION > 30
#include <fuse3/fuse.h>
#else
#include <fuse/fuse.h>
#endif
#include <sys/stat.h>
#include <sys/xattr.h>

#if defined(__linux__)
#include <sys/fsuid.h>
#endif

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>

#if defined(__APPLE__)
/**
 * These funtions provide drop-in replacements for setfsuid and setfsgid on OSX
 */
static inline int setfsuid(uid_t uid)
{
    LOG_FCALL() << LOG_FARG(uid);

    uid_t olduid = geteuid();

    seteuid(uid);

    if (errno != EINVAL)
        errno = 0;

    return olduid;
}

static inline int setfsgid(gid_t gid)
{
    LOG_FCALL() << LOG_FARG(gid);

    gid_t oldgid = getegid();

    setegid(gid);

    if (errno != EINVAL)
        errno = 0;

    return oldgid;
}
#endif

namespace {

// Retry only in case one of these errors occured
const std::set<int> &POSIXRetryErrors()
{
    static const std::set<int> POSIX_RETRY_ERRORS = {
        EINTR,
        EIO,
        EAGAIN,
        EACCES,
        EBUSY,
        EMFILE,
        ETXTBSY,
        ESPIPE,
        EMLINK,
        EPIPE,
        EDEADLK,
        EWOULDBLOCK,
        ENOLINK,
        EADDRINUSE,
        EADDRNOTAVAIL,
        ENETDOWN,
        ENETUNREACH,
        ECONNABORTED,
        ECONNRESET,
        ENOTCONN,
        EHOSTUNREACH,
        ECANCELED,
        ESTALE
#if !defined(__APPLE__)
        ,
        ENONET,
        EHOSTDOWN,
        EREMOTEIO,
        ENOMEDIUM
#endif
    };
    return POSIX_RETRY_ERRORS;
}

inline bool POSIXRetryCondition(int result, const std::string &operation)
{
    auto ret = (result >= 0 ||
        POSIXRetryErrors().find(errno) == POSIXRetryErrors().end());

    if (!ret) {
        LOG(WARNING) << "Retrying POSIX helper operation '" << operation
                     << "' due to error: " << errno;
        ONE_METRIC_COUNTER_INC(
            "comp.helpers.mod.posix." + operation + ".retries");
    }

    return ret;
}

template <typename... Args1, typename... Args2>
inline void setResult(folly::Promise<folly::Unit> &promise,
    const std::string &operation, int (*fun)(Args2...), Args1 &&...args)
{
    auto ret =
        one::helpers::retry([&]() { return fun(std::forward<Args1>(args)...); },
            std::bind(POSIXRetryCondition, std::placeholders::_1, operation));

    if (ret < 0)
        promise.setException(one::helpers::makePosixException(errno));
    else
        promise.setValue();
}

template <typename... Args1, typename... Args2>
inline folly::Future<folly::Unit> setResult(
    const std::string &operation, int (*fun)(Args2...), Args1 &&...args)
{
    auto ret =
        one::helpers::retry([&]() { return fun(std::forward<Args1>(args)...); },
            std::bind(POSIXRetryCondition, std::placeholders::_1, operation));

    if (ret < 0)
        return one::helpers::makeFuturePosixException(errno);

    return folly::makeFuture();
}

} // namespace

namespace one {
namespace helpers {

#if defined(__linux__) || defined(__APPLE__)
UserCtxSetter::UserCtxSetter(const uid_t uid, const gid_t gid)
    : m_uid{uid}
    , m_gid{gid}
    , m_prevUid{static_cast<uid_t>(setfsuid(uid))}
    , m_prevGid{static_cast<gid_t>(setfsgid(gid))}
    , m_currUid{static_cast<uid_t>(setfsuid(-1))}
    , m_currGid{static_cast<gid_t>(setfsgid(-1))}
{
}

UserCtxSetter::~UserCtxSetter()
{
    setfsuid(m_prevUid);
    setfsgid(m_prevGid);
}

bool UserCtxSetter::valid() const
{
    return (m_uid == static_cast<uid_t>(-1) || m_currUid == m_uid) &&
        (m_gid == static_cast<gid_t>(-1) || m_currGid == m_gid);
}
#else
UserCtxSetter::UserCtxSetter(const int, const int) { }
UserCtxSetter::~UserCtxSetter() { }
bool UserCtxSetter::valid() const { return true; }
#endif

std::shared_ptr<PosixFileHandle> PosixFileHandle::create(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid,
    const int fileHandle, std::shared_ptr<PosixHelper> helper,
    std::shared_ptr<folly::Executor> executor, Timeout timeout)
{
    auto ptr = std::shared_ptr<PosixFileHandle>(new PosixFileHandle(fileId, uid,
        gid, fileHandle, std::move(helper), std::move(executor), timeout));
    ptr->initOpScheduler(ptr);
    return ptr;
}

PosixFileHandle::PosixFileHandle(const folly::fbstring &fileId, const uid_t uid,
    const gid_t gid, const int fileHandle, std::shared_ptr<PosixHelper> helper,
    std::shared_ptr<folly::Executor> executor, Timeout timeout)
    : FileHandle{fileId, std::move(helper)}
    , m_uid{uid}
    , m_gid{gid}
    , m_fh{fileHandle}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid)
                << LOG_FARG(fileHandle);
}

void PosixFileHandle::initOpScheduler(std::shared_ptr<PosixFileHandle> self)
{
    opScheduler = FlatOpScheduler<HandleOp, OpExec>::create(
        m_executor, std::make_shared<OpExec>(self));
}

PosixFileHandle::~PosixFileHandle()
{
    LOG_FCALL();

    if (m_needsRelease.exchange(false)) {
        UserCtxSetter userCTX{m_uid, m_gid};
        if ((helper()->executionContext() == ExecutionContext::ONEPROVIDER) &&
            !userCTX.valid()) {
            LOG(WARNING) << "Failed to release file " << m_fh
                         << ": failed to set user context";
            return;
        }

        if (close(m_fh) == -1) {
            auto ec = makePosixError(errno);
            LOG(WARNING) << "Failed to release file " << m_fh << ": "
                         << ec.message();
        }
    }
}

PosixFileHandle::OpExec::OpExec(const std::shared_ptr<PosixFileHandle> &handle)
    : m_handle{handle}
{
}

std::unique_ptr<UserCtxSetter> PosixFileHandle::OpExec::startDrain()
{
    if (auto handle = m_handle.lock()) {
        auto userCtx =
            std::make_unique<UserCtxSetter>(handle->m_uid, handle->m_gid);
        m_validCtx = (handle->helper()->executionContext() ==
                         ExecutionContext::ONECLIENT) ||
            userCtx->valid();
        return userCtx;
    }

    return std::unique_ptr<UserCtxSetter>{nullptr};
}

void PosixFileHandle::OpExec::operator()(ReadOp &op) const
{
    if (!m_validCtx) {
        op.promise.setException(makePosixException(EDOM));
        return;
    }

    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    log_timer<> logTimer;

    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};

    void *data = buf.preallocate(op.size, op.size).first;

    LOG_DBG(2) << "Attempting to read " << op.size << " bytes at offset "
               << op.offset << " from file " << handle->fileId();

    auto res =
        retry([&]() { return ::pread(handle->m_fh, data, op.size, op.offset); },
            std::bind(POSIXRetryCondition, std::placeholders::_1, "pread"));

    if (res == -1) {
        LOG_DBG(1) << "Reading from file " << handle->fileId()
                   << " failed with error " << errno;
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.errors.read");
        op.promise.setException(makePosixException(errno));
        return;
    }

    buf.postallocate(res);

    log<read_write_perf>(handle->fileId(), "PosixHelper", "read", op.offset,
        op.size, logTimer.stop());

    LOG_DBG(2) << "Read " << res << " bytes from file " << handle->fileId();

    ONE_METRIC_TIMERCTX_STOP(op.timer, res);

    op.promise.setValue(std::move(buf));
}

void PosixFileHandle::OpExec::operator()(WriteOp &op) const
{
    if (!m_validCtx) {
        op.promise.setException(makePosixException(EDOM));
        return;
    }

    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    log_timer<> logTimer;

    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    auto res = ::lseek(handle->m_fh, op.offset, SEEK_SET);
    if (res == -1) {
        op.promise.setException(makePosixException(errno));
        return;
    }

    if (op.buf.empty()) {
        op.promise.setValue(0);
        return;
    }

    auto iobuf = op.buf.empty() ? folly::IOBuf::create(0) : op.buf.move();
    if (iobuf->isChained()) {
        LOG_DBG(2) << "Coalescing chained buffer at offset " << op.offset
                   << " of size: " << iobuf->length();
        iobuf->unshare();
        iobuf->coalesce();
    }

    auto iov = iobuf->getIov();
    auto iov_size = iov.size();
    auto size = 0;

    LOG_DBG(2) << "Attempting to write " << iobuf->length()
               << " bytes at offset " << op.offset << " to file "
               << handle->fileId();

    for (std::size_t iov_off = 0; iov_off < iov_size; iov_off += IOV_MAX) {
        res = retry(
            [&]() {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                return ::writev(handle->m_fh, iov.data() + iov_off,
                    std::min<std::size_t>(IOV_MAX, iov_size - iov_off));
            },
            [](int result) { return result != -1; });

        if (res == -1) {
            LOG(ERROR) << "Writing to file " << handle->fileId()
                       << " failed with error " << errno;
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.errors.write");
            op.promise.setException(makePosixException(errno));
            return;
        }
        size += res;
    }

    log<read_write_perf>(handle->fileId(), "PosixHelper", "write", op.offset,
        size, logTimer.stop());

    LOG_DBG(2) << "Written " << size << " bytes to file " << handle->fileId()
               << " at offset " << op.offset;

    ONE_METRIC_TIMERCTX_STOP(op.timer, size);

    op.promise.setValue(size);
}

void PosixFileHandle::OpExec::operator()(ReleaseOp &op) const
{
    if (!m_validCtx) {
        op.promise.setException(makePosixException(EDOM));
        return;
    }

    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.release");
    LOG_DBG(2) << "Closing file " << handle->fileId();
    setResult(op.promise, "close", close, handle->m_fh);
}

void PosixFileHandle::OpExec::operator()(FsyncOp &op) const
{
    if (!m_validCtx) {
        op.promise.setException(makePosixException(EDOM));
        return;
    }

    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.fsync");
    LOG_DBG(2) << "Syncing file " << handle->fileId();
    setResult(op.promise, "fsync", ::fsync, handle->m_fh);
}

void PosixFileHandle::OpExec::operator()(FlushOp &op) const
{
    if (!m_validCtx) {
        op.promise.setException(makePosixException(EDOM));
        return;
    }

    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.flush");
    LOG_DBG(2) << "Flushing file " << handle->fileId();
    op.promise.setValue();
}

folly::Future<folly::IOBufQueue> PosixFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);
    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.posix.read");
    return opScheduler->schedule(ReadOp{{}, offset, size, std::move(timer)});
}

folly::Future<std::size_t> PosixFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());
    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.posix.write");
    return opScheduler
        ->schedule(WriteOp{{}, offset, std::move(buf), std::move(timer)})
        .thenValue([writeCb = std::move(writeCb)](std::size_t &&written) {
            if (writeCb)
                writeCb(written);
            return written;
        });
}

folly::Future<folly::Unit> PosixFileHandle::release()
{
    LOG_FCALL();

    if (!m_needsRelease.exchange(false))
        return folly::makeFuture();

    return opScheduler->schedule(ReleaseOp{});
}

folly::Future<folly::Unit> PosixFileHandle::flush()
{
    LOG_FCALL();
    return opScheduler->schedule(FlushOp{});
}

folly::Future<folly::Unit> PosixFileHandle::fsync(bool /*isDataSync*/)
{
    LOG_FCALL();
    return opScheduler->schedule(FsyncOp{});
}

PosixHelper::PosixHelper(std::shared_ptr<PosixHelperParams> params,
    std::shared_ptr<folly::Executor> executor,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_executor{std::move(executor)}
{
    LOG_FCALL();

    invalidateParams()->setValue(std::move(params));
}

folly::Future<folly::Unit> PosixHelper::checkStorageAvailability()
{
    LOG_FCALL();

    return readdir("/", 0, 1).thenValue(
        [](auto && /*unused*/) { return folly::makeFuture(); });
}

folly::Future<struct stat> PosixHelper::getattr(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [filePath = root(fileId), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.getattr");

            struct stat stbuf = {};

            LOG_DBG(2) << "Attempting to stat file " << filePath;

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException<struct stat>(EDOM);

            auto res = retry(
                [&]() { return ::lstat(filePath.c_str(), &stbuf); },
                std::bind(POSIXRetryCondition, std::placeholders::_1, "lstat"));

            if (res == -1) {
                LOG_DBG(1) << "Stating file " << filePath
                           << " failed with error " << errno;
                return makeFuturePosixException<struct stat>(errno);
            }

            return folly::makeFuture(stbuf);
        });
}

folly::Future<folly::Unit> PosixHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mask);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), mask, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.access");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            LOG_DBG(2) << "Attempting to access file " << filePath;

            return setResult("access", ::access, filePath.c_str(), mask);
        });
}

folly::Future<folly::fbvector<folly::fbstring>> PosixHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), offset, count, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.readdir");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(EDOM);

            folly::fbvector<folly::fbstring> ret;

            LOG_DBG(2) << "Attempting to read directory " << filePath;

            DIR *dir{nullptr};
            struct dirent *dp{nullptr};
            dir = retry([&]() { return opendir(filePath.c_str()); },
                [](DIR *d) {
                    return d != nullptr ||
                        POSIXRetryErrors().find(errno) ==
                        POSIXRetryErrors().end();
                });

            if (dir == nullptr) {
                LOG_DBG(1) << "Opening directory " << filePath
                           << " failed with error " << errno;
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(errno);
            }

            int offset_ = offset;
            int count_ = count;
            while ((dp = retry([&]() { return ::readdir(dir); },
                        [](struct dirent *de) {
                            return de != nullptr ||
                                POSIXRetryErrors().find(errno) ==
                                POSIXRetryErrors().end();
                        })) != nullptr &&
                count_ > 0) {
                if (strcmp(static_cast<char *>(dp->d_name), ".") != 0 &&
                    strcmp(static_cast<char *>(dp->d_name), "..") != 0) {
                    if (offset_ > 0) {
                        --offset_;
                    }
                    else {
                        ret.push_back(
                            folly::fbstring(static_cast<char *>(dp->d_name)));
                        --count_;
                    }
                }
            }
            closedir(dir);

            LOG_DBG(2) << "Read directory " << filePath << " at offset "
                       << offset << " with entries " << LOG_VEC(ret);

            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(ret));
        });
}

folly::Future<folly::fbstring> PosixHelper::readlink(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [filePath = root(fileId), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.readlink");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException<folly::fbstring>(EDOM);

            LOG_DBG(1) << "Attempting to read link " << filePath;

            constexpr std::size_t maxSize = 1024;
            auto buf = folly::IOBuf::create(maxSize);
            auto res = retry(
                [&]() {
                    return ::readlink(filePath.c_str(),
                        reinterpret_cast<char *>(buf->writableData()),
                        maxSize - 1);
                },
                std::bind(
                    POSIXRetryCondition, std::placeholders::_1, "readlink"));

            if (res == -1) {
                LOG_DBG(1) << "Reading link " << filePath
                           << " failed with error " << errno;
                return makeFuturePosixException<folly::fbstring>(errno);
            }

            buf->append(res);

            auto target = buf->moveToFbString();

            LOG_DBG(2) << "Read link " << filePath << " - resolves to "
                       << target;

            return folly::makeFuture(std::move(target));
        });
}

folly::Future<folly::Unit> PosixHelper::mknod(const folly::fbstring &fileId,
    const mode_t unmaskedMode, const FlagsSet &flags, const dev_t rdev)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(unmaskedMode)
                << LOG_FARG(flagsToMask(flags));

    const mode_t mode = unmaskedMode | flagsToMask(flags);
    return folly::via(m_executor.get(),
        [filePath = root(fileId), mode, rdev, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.mknod");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            int res{0};

            /* On Linux this could just be 'mknod(path, mode, rdev)' but
               this is more portable */
            if (S_ISREG(mode)) {
                res = retry(
                    [&]() {
                        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
                        return ::open(filePath.c_str(),
                            O_CREAT | O_EXCL | O_WRONLY, mode);
                    },
                    std::bind(
                        POSIXRetryCondition, std::placeholders::_1, "open"));

                if (res >= 0)
                    res = close(res);
            }
            else if (S_ISFIFO(mode)) {
                res = retry([&]() { return ::mkfifo(filePath.c_str(), mode); },
                    std::bind(
                        POSIXRetryCondition, std::placeholders::_1, "mkfifo"));
            }
            else {
                res = retry(
                    [&]() { return ::mknod(filePath.c_str(), mode, rdev); },
                    std::bind(
                        POSIXRetryCondition, std::placeholders::_1, "mknod"));
            }

            if (res == -1)
                return makeFuturePosixException(errno);

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> PosixHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), mode, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.mkdir");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("mkdir", ::mkdir, filePath.c_str(), mode);
        });
}

folly::Future<folly::Unit> PosixHelper::unlink(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [filePath = root(fileId), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.unlink");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("unlink", ::unlink, filePath.c_str());
        });
}

folly::Future<folly::Unit> PosixHelper::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [filePath = root(fileId), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.rmdir");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("rmdir", ::rmdir, filePath.c_str());
        });
}

folly::Future<folly::Unit> PosixHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return folly::via(m_executor.get(),
        [from = root(from), to = root(to), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.symlink");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("symlink", ::symlink, from.c_str(), to.c_str());
        });
}

folly::Future<folly::Unit> PosixHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return folly::via(m_executor.get(),
        [from = root(from), to = root(to), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.rename");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("rename", ::rename, from.c_str(), to.c_str());
        });
}

folly::Future<folly::Unit> PosixHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return folly::via(m_executor.get(),
        [from = root(from), to = root(to), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.link");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("link", ::link, from.c_str(), to.c_str());
        });
}

folly::Future<folly::Unit> PosixHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), mode, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.chmod");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("chmod", ::chmod, filePath.c_str(), mode);
        });
}

folly::Future<folly::Unit> PosixHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), argUid = uid, argGid = gid, uid = this->uid(),
            gid = this->gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.chown");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult(
                "chown", ::chown, filePath.c_str(), argUid, argGid);
        });
}

folly::Future<folly::Unit> PosixHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), size, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.truncate");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult("truncate", ::truncate, filePath.c_str(), size);
        });
}

folly::Future<FileHandlePtr> PosixHelper::open(const folly::fbstring &fileId,
    const int flags, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(flags);

    return folly::via(m_executor.get(),
        [fileId, filePath = root(fileId), flags, executor = m_executor,
            uid = uid(), gid = gid(), self = shared_from_this(),
            timeout = timeout()]() mutable {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.open");

            UserCtxSetter userCTX{uid, gid};
            if ((self->executionContext() == ExecutionContext::ONEPROVIDER) &&
                !userCTX.valid())
                return makeFuturePosixException<FileHandlePtr>(EDOM);

            int res = retry(
                [&]() {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
                    return ::open(filePath.c_str(), flags);
                },
                std::bind(POSIXRetryCondition, std::placeholders::_1, "open"));

            if (res == -1)
                return makeFuturePosixException<FileHandlePtr>(errno);

            auto handle = PosixFileHandle::create(fileId, uid, gid, res,
                std::move(self), std::move(executor), timeout);

            return folly::makeFuture<FileHandlePtr>(std::move(handle));
        });
}

folly::Future<folly::fbstring> PosixHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), name, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.getxattr");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException<folly::fbstring>(EDOM);

            constexpr std::size_t initialMaxSize = 256;
            auto buf = folly::IOBuf::create(initialMaxSize);

            int res = retry(
                [&]() {
                    return ::getxattr(filePath.c_str(), name.c_str(),
                        reinterpret_cast<char *>(buf->writableData()),
                        initialMaxSize - 1
#if defined(__APPLE__)
                        ,
                        0, 0
#endif
                    );
                },
                std::bind(
                    POSIXRetryCondition, std::placeholders::_1, "getxattr"));
            // If the initial buffer for xattr value was too small, try
            // again with maximum allowed value
            if (res == -1 && errno == ERANGE) {
                buf = folly::IOBuf::create(XATTR_SIZE_MAX);
                res = retry(
                    [&]() {
                        return ::getxattr(filePath.c_str(), name.c_str(),
                            reinterpret_cast<char *>(buf->writableData()),
                            XATTR_SIZE_MAX - 1
#if defined(__APPLE__)
                            ,
                            0, 0
#endif
                        );
                    },
                    std::bind(POSIXRetryCondition, std::placeholders::_1,
                        "getxattr"));
            }

            if (res == -1)
                return makeFuturePosixException<folly::fbstring>(errno);

            buf->append(res);
            return folly::makeFuture(buf->moveToFbString());
        });
}

folly::Future<folly::Unit> PosixHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name) << LOG_FARG(value)
                << LOG_FARG(create) << LOG_FARG(replace);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), name, value, create, replace, uid = uid(),
            gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.setxattr");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException<folly::Unit>(EDOM);

            int flags = 0;

            if (create && replace) {
                return makeFuturePosixException<folly::Unit>(EINVAL);
            }

            if (create) {
                flags = XATTR_CREATE;
            }
            else if (replace) {
                flags = XATTR_REPLACE;
            }

            return setResult("setxattr", ::setxattr, filePath.c_str(),
                name.c_str(), value.c_str(), value.size(),
#if defined(__APPLE__)
                0,
#endif
                flags);
        });
}

folly::Future<folly::Unit> PosixHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return folly::via(m_executor.get(),
        [filePath = root(fileId), name, uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.removexattr");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException(EDOM);

            return setResult(
                "removexattr", ::removexattr, filePath.c_str(), name.c_str()
#if defined(__APPLE__)
                                                                    ,
                0
#endif
            );
        });
}

folly::Future<folly::fbvector<folly::fbstring>> PosixHelper::listxattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [filePath = root(fileId), uid = uid(), gid = gid()] {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.posix.listxattr");

            UserCtxSetter userCTX{uid, gid};
            if (!userCTX.valid())
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(EDOM);

            folly::fbvector<folly::fbstring> ret;

            ssize_t buflen = retry(
                [&]() {
                    return ::listxattr(filePath.c_str(), nullptr, 0
#if defined(__APPLE__)
                        ,
                        0
#endif
                    );
                },
                std::bind(
                    POSIXRetryCondition, std::placeholders::_1, "listxattr"));

            if (buflen == -1)
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(errno);

            if (buflen == 0)
                return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                    std::move(ret));

            auto buf = std::unique_ptr<char[]>(new char[buflen]); // NOLINT
            buflen = ::listxattr(filePath.c_str(), buf.get(), buflen
#if defined(__APPLE__)
                ,
                0
#endif
            );

            if (buflen == -1)
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(errno);

            char *xattrNamePtr = buf.get();
            while (xattrNamePtr < buf.get() + buflen) {
                ret.emplace_back(xattrNamePtr);
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                xattrNamePtr +=
                    strnlen(xattrNamePtr, buflen - (buf.get() - xattrNamePtr)) +
                    1;
            }

            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(ret));
        });
}

} // namespace helpers
} // namespace one
