/**
 * @file xrootdHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "xrootdHelper.h"
#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <XProtocol/XProtocol.hh>
#include <XrdCl/XrdClFileOperations.hh>
#include <XrdCl/XrdClFileSystemOperations.hh>

#include <set>

namespace one {
namespace helpers {

namespace {

const std::set<int> XROOTD_RETRY_ERRORS = {
    kXR_Cancelled, kXR_inProgress, kXR_Overloaded, kXR_FileLocked};

inline bool shouldRetryError(const XrdCl::PipelineException &ex)
{
    auto ec = ex.GetError().errNo;
    return XROOTD_RETRY_ERRORS.find(ec) != XROOTD_RETRY_ERRORS.cend();
}

const auto kXRootDRetryMinimumDelay = std::chrono::milliseconds{5};

inline auto retryDelay(int retriesLeft)
{
    const unsigned int kXRootDRetryBaseDelay_ms = 100;
    return kXRootDRetryMinimumDelay +
        std::chrono::milliseconds{kXRootDRetryBaseDelay_ms *
            (kXRootDRetryCount - retriesLeft) *
            (kXRootDRetryCount - retriesLeft)};
}

/**
 * Prepend the relative file or directory path with the path registered as path
 * of the Url helper param and ensure it's absolute as required by XRootD FS
 * operations.
 */
inline std::string ensureAbsPath(
    const std::string &root, const std::string &path)
{
    if (root.empty())
        return ensureAbsPath("/", path);

    if (root.back() != '/' && path.front() != '/')
        return root + '/' + path;

    return root + path;
}

/**
 * Make sure that the path is relative, i.e. not starting with '/', as required
 * by some XRootD operations
 */
inline std::string ensureRelPath(const std::string &path)
{
    if (path.empty())
        return path;

    if (path.front() == '/')
        return path.substr(1);

    return path;
}

/**
 * Convert POSIX flags to XRootD open flags.
 */
inline auto flagsToOpenFlags(const int flags)
{
    if (flags & O_RDONLY)
        return XrdCl::OpenFlags::Read;
    else if (flags & O_WRONLY)
        return XrdCl::OpenFlags::Write;
    else if (flags & O_RDWR)
        return XrdCl::OpenFlags::Write;
    else if (flags & O_CREAT)
        return XrdCl::OpenFlags::New;
    else
        return XrdCl::OpenFlags::Read;
}

/**
 * Convert POSIX mode to XRootD Access::Mode.
 */
inline auto modeToAccess(const mode_t mode)
{
    using XrdCl::Access;

    Access::Mode access{Access::None};

    if (mode & S_IRUSR)
        access |= Access::UR;
    if (mode & S_IWUSR)
        access |= Access::UW;
    if (mode & S_IXUSR)
        access |= Access::UX;

    if (mode & S_IRGRP)
        access |= Access::GR;
    if (mode & S_IWGRP)
        access |= Access::GW;
    if (mode & S_IXGRP)
        access |= Access::GX;

    if (mode & S_IROTH)
        access |= Access::OR;
    if (mode & S_IWOTH)
        access |= Access::OW;
    if (mode & S_IXOTH)
        access |= Access::OX;

    return access;
}

/**
 * Convert XRootD Status Code to appropriate POSIX error
 */
static int xrootdStatusToPosixError(const XrdCl::XRootDStatus &xrootdStatus)
{
    switch (xrootdStatus.errNo) {
        case kXR_AuthFailed:
        case kXR_NotAuthorized:
            return EACCES;
        case kXR_FileLocked:
        case kXR_inProgress:
            return EINPROGRESS;
        case kXR_Cancelled:
            return EAGAIN;
        case kXR_BadPayload:
            return EBADMSG;
        case kXR_InvalidRequest:
        case kXR_ArgInvalid:
            return EINVAL;
        case kXR_NotFound:
            return ENOENT;
        case kXR_ItExists:
            return EEXIST;
        case kXR_NoSpace:
            return EDQUOT;
        case kXR_fsReadOnly:
            return EPERM;
        case kXR_isDirectory:
            return EISDIR;
        case kXR_Unsupported:
            return ENOTSUP;
        case kXR_IOError:
        default:
            return EIO;
    }
}
}

XRootDFileHandle::XRootDFileHandle(folly::fbstring fileId,
    std::unique_ptr<XrdCl::File> &&file, std::shared_ptr<XRootDHelper> helper)
    : FileHandle{fileId, std::move(helper)}
    , m_file{std::move(file)}
{
}

folly::Future<folly::IOBufQueue> XRootDFileHandle::read(
    const off_t offset, const std::size_t size)
{
    return read(offset, size, kXRootDRetryCount);
}

folly::Future<folly::IOBufQueue> XRootDFileHandle::read(
    const off_t offset, const std::size_t size, const int retryCount)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    assert(m_file->IsOpen());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.xrootd.read");

    LOG_DBG(2) << "Attempting to read " << size << " bytes at offset " << offset
               << " from file " << fileId();

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
    char *data = static_cast<char *>(buf.preallocate(size, size).first);

    auto p = folly::Promise<std::size_t>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st, XrdCl::ChunkInfo & info)>
        readTask{[p = std::move(p)](XrdCl::XRootDStatus & st,
            XrdCl::ChunkInfo & info) mutable {p.setWith([&st, &info]() {
            if (!st.IsOK())
                throw XrdCl::PipelineException(st);
            return static_cast<std::size_t>(info.length);
        });
}
};

auto tf =
    XrdCl::Async(XrdCl::Read(m_file.get(), offset, size, data) >> readTask);

return f
    .then(helper()->executor().get(),
        [
            buf = std::move(buf), offset, size, fileId = fileId(),
            timer = std::move(timer), tf = std::move(tf)
        ](std::size_t readBytes) mutable {
            buf.postallocate(static_cast<std::size_t>(readBytes));
            ONE_METRIC_TIMERCTX_STOP(timer, readBytes);
            return folly::makeFuture(std::move(buf));
        })
    .onError([
        retryCount, offset, size, fileId = fileId(),
        s = std::weak_ptr<XRootDFileHandle>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::IOBufQueue>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.read.retries")
            return folly::via(self->helper()->executor().get())
                .delayed(retryDelay(retryCount))
                .then([
                    self = std::move(self), size, offset, retryCount
                ]() mutable {
                    return self->read(offset, size, retryCount - 1);
                });
        }

        ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.errors.read");

        LOG(ERROR) << "Read from file " << fileId << " failed due to "
                   << ex.GetError().GetErrorMessage() << ":"
                   << ex.GetError().code;

        return makeFuturePosixException<folly::IOBufQueue>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<std::size_t> XRootDFileHandle::write(
    const off_t offset, folly::IOBufQueue buf)
{
    auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();
    if (iobuf->isChained()) {
        iobuf->unshare();
        iobuf->coalesce();
    }

    folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
    queue.append(iobuf->cloneOne());

    return write(offset, std::move(queue), kXRootDRetryCount);
}

folly::Future<std::size_t> XRootDFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, const int retryCount)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    assert(m_file->IsOpen());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.xrootd.write");

    auto data = reinterpret_cast<const void *>(buf.front()->data());
    auto size = buf.front()->length();

    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> writeTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                return folly::Unit();
            });
}
}
;

auto tf =
    XrdCl::Async(XrdCl::Write(m_file.get(), static_cast<uint64_t>(offset),
                     static_cast<uint32_t>(size), const_cast<void *>(data)) |
        XrdCl::Sync(m_file.get()) >> writeTask);

return f
    .then(helper()->executor().get(),
        [ size, tf = std::move(tf), timer = std::move(timer) ]() mutable {
            ONE_METRIC_TIMERCTX_STOP(timer, size);
            return folly::makeFuture(size);
        })
    .onError([
        retryCount, offset, fileId = fileId(), buf = std::move(buf),
        s = std::weak_ptr<XRootDFileHandle>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<std::size_t>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.write.retries")
            return folly::via(self->helper()->executor().get())
                .delayed(retryDelay(retryCount))
                .then([
                    self = std::move(self), buf = std::move(buf), offset,
                    retryCount
                ]() mutable {
                    return self->write(offset, std::move(buf), retryCount - 1);
                });
        }

        ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.errors.write");

        LOG(ERROR) << "Write to file " << fileId << " failed due to "
                   << ex.GetError().GetErrorMessage() << ":"
                   << ex.GetError().code;

        return makeFuturePosixException<std::size_t>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::Unit> XRootDFileHandle::release()
{
    return release(kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDFileHandle::release(const int retryCount)
{
    LOG_FCALL();

    if (!m_file->IsOpen())
        return {};

    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> closeTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
            });
}
}
;

auto tf = XrdCl::Async(XrdCl::Close(m_file.get()) >> closeTask);

return f
    .then(helper()->executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        retryCount, s = std::weak_ptr<XRootDFileHandle>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.release.retries")
            return folly::via(self->helper()->executor().get())
                .delayed(retryDelay(retryCount))
                .then([ self = std::move(self), retryCount ]() mutable {
                    return self->release(retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
return {};
}

folly::Future<folly::Unit> XRootDFileHandle::fsync(bool isDataSync)
{
    return fsync(isDataSync, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDFileHandle::fsync(
    bool isDataSync, const int retryCount)
{
    assert(m_file->IsOpen());

    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> syncTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
            });
}
}
;

auto tf = XrdCl::Async(XrdCl::Sync(m_file.get()) >> syncTask);

return f
    .then(helper()->executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        retryCount, isDataSync,
        s = std::weak_ptr<XRootDFileHandle>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.fsync.retries")
            return folly::via(self->helper()->executor().get())
                .delayed(retryDelay(retryCount))
                .then([
                    self = std::move(self), isDataSync, retryCount
                ]() mutable {
                    return self->fsync(isDataSync, retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

const Timeout &XRootDFileHandle::timeout() { return m_helper->timeout(); }

XRootDHelper::XRootDHelper(std::shared_ptr<XRootDHelperParams> params,
    std::shared_ptr<folly::IOExecutor> executor)
    : m_executor{std::move(executor)}
    , m_fs{params->url()}
{
    invalidateParams()->setValue(std::move(params));
}

XRootDHelper::~XRootDHelper() {}

folly::Future<folly::Unit> XRootDHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    return access(fileId, mask, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::access(
    const folly::fbstring &fileId, const int mask, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(
        XrdCl::XRootDStatus & st, XrdCl::LocationInfo & info)>
        locateTask{[p = std::move(p)](XrdCl::XRootDStatus & st,
            XrdCl::LocationInfo & info) mutable {p.setWith([&st, &info]() {
            if (!st.IsOK())
                throw XrdCl::PipelineException(st);
            return folly::Unit();
        });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::Locate(m_fs, ensureAbsPath(url().GetPath(), fileId.toStdString()),
        XrdCl::OpenFlags::Read) >>
    locateTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        fileId, retryCount, s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.access.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([ self = std::move(self), fileId, retryCount ]() mutable {
                    return self->access(fileId, retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<struct stat> XRootDHelper::getattr(const folly::fbstring &fileId)
{
    return getattr(fileId, kXRootDRetryCount);
}

folly::Future<struct stat> XRootDHelper::getattr(
    const folly::fbstring &fileId, const int retryCount)
{
    auto p = folly::Promise<struct stat>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st, XrdCl::StatInfo & info)>
        statTask{[p = std::move(p)](XrdCl::XRootDStatus & st,
            XrdCl::StatInfo & info) mutable {p.setWith([&st, &info]() {
            if (!st.IsOK())
                throw XrdCl::PipelineException(st);

            struct stat attr = {};
            attr.st_size = info.GetSize();

            const auto &flags = info.GetFlags();
            if (flags & kXR_isDir)
                attr.st_mode = S_IFDIR;
            else if (flags & kXR_other)
                attr.st_mode = S_IFIFO;
            else
                attr.st_mode = S_IFREG;

            if (flags & kXR_readable)
                attr.st_mode |= S_IRGRP | S_IRUSR;

            if (flags & kXR_writable)
                attr.st_mode |= S_IWGRP | S_IWUSR;

            if (flags & kXR_xset)
                attr.st_mode |= S_IXGRP | S_IXUSR;

            attr.st_mtim.tv_sec = info.GetModTime();
            attr.st_mtim.tv_nsec = 0;
            attr.st_ctim = attr.st_mtim;
            attr.st_atim.tv_sec = info.GetAccessTime();
            attr.st_atim.tv_nsec = 0;

            return attr;
        });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::Stat(m_fs, ensureAbsPath(url().GetPath(), fileId.toStdString())) >>
    statTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)](struct stat &&attr) mutable {
            return folly::makeFuture<struct stat>(std::move(attr));
        })
    .onError([
        fileId, retryCount, s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<struct stat>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.getattr.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([ self = std::move(self), fileId, retryCount ]() mutable {
                    return self->getattr(fileId, retryCount - 1);
                });
        }

        return makeFuturePosixException<struct stat>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<FileHandlePtr> XRootDHelper::open(const folly::fbstring &fileId,
    const int flags, const Params & /*openParams*/)
{
    auto p = folly::Promise<std::shared_ptr<XRootDFileHandle>>();
    auto f = p.getFuture();

    auto file = std::make_unique<XrdCl::File>(true);
    auto filePtr = file.get();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> openTask{
        [this, p = std::move(p), fileId, file = std::move(file)](
            XrdCl::XRootDStatus & st) mutable {
            p.setWith([
                &st, fileId, file = std::move(file),
                s = std::weak_ptr<XRootDHelper>{shared_from_this()}
            ]() mutable {
                auto self = s.lock();
                if (!self)
                    throw makePosixException(ECANCELED);

                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                return std::make_shared<XRootDFileHandle>(
                    fileId, std::move(file), std::move(self));
            });
}
}
;

auto tf =
    XrdCl::Async(XrdCl::Open(filePtr, url().GetURL() + fileId.toStdString(),
                     flagsToOpenFlags(flags)) >>
        openTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)](
            std::shared_ptr<XRootDFileHandle> &&handle) mutable {
            return folly::makeFuture<FileHandlePtr>(std::move(handle));
        })
    .onError([ fileId, s = std::weak_ptr<XRootDHelper>{shared_from_this()} ](
        const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<FileHandlePtr>(ECANCELED);

        LOG(ERROR) << "Open of file " << fileId << " failed due to "
                   << ex.GetError().GetErrorMessage() << ":"
                   << ex.GetError().errNo;

        return makeFuturePosixException<FileHandlePtr>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::Unit> XRootDHelper::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    return unlink(fileId, currentSize, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::unlink(const folly::fbstring &fileId,
    const size_t currentSize, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> rmTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::Rm(m_fs, ensureAbsPath(url().GetPath(), fileId.toStdString())) >>
    rmTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() { return folly::makeFuture(); })
    .onError([
        fileId, retryCount, s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        LOG(ERROR) << "Rm of file " << fileId << " failed due to "
                   << ex.GetError().GetErrorMessage() << ":"
                   << ex.GetError().errNo;

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.unlink.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([ self = std::move(self), fileId, retryCount ]() mutable {
                    return self->unlink(fileId, retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::Unit> XRootDHelper::rmdir(const folly::fbstring &fileId)
{
    return rmdir(fileId, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::rmdir(
    const folly::fbstring &fileId, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> rmDirTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::RmDir(m_fs, ensureAbsPath(url().GetPath(), fileId.toStdString())) >>
    rmDirTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() { return folly::makeFuture(); })
    .onError([
        fileId, retryCount, s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.unlink.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([ self = std::move(self), fileId, retryCount ]() mutable {
                    return self->rmdir(fileId, retryCount - 1);
                });
        }

        LOG(ERROR) << "RmDir failed due to: "
                   << ex.GetError().GetErrorMessage();

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::Unit> XRootDHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    return truncate(fileId, size, currentSize, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t currentSize, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> truncateTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::Truncate(m_fs, ensureRelPath(fileId.toStdString()), size) >>
    truncateTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        fileId, retryCount, size, currentSize,
        s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.unlink.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([
                    self = std::move(self), fileId, size, currentSize,
                    retryCount
                ]() mutable {
                    return self->truncate(
                        fileId, size, currentSize, retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::Unit> XRootDHelper::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet &flags, const dev_t rdev)
{
    return mknod(fileId, mode, flags, rdev, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet &flags, const dev_t rdev,
    const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    auto file = std::make_unique<XrdCl::File>(true);
    auto filePtr = file.get();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> &&openTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() mutable {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
            });
}
}
;

LOG(ERROR) << "Creating file: " << url().GetURL() + fileId.toStdString();
auto tf =
    XrdCl::Async(XrdCl::Open(filePtr, url().GetURL() + fileId.toStdString(),
                     XrdCl::OpenFlags::New, modeToAccess(mode)) |
        XrdCl::Sync(filePtr) | XrdCl::Close(filePtr) >> openTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        fileId, file = std::move(file),
        s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        LOG(ERROR) << "Creation of file " << fileId
                   << " failed due to: " << ex.GetError().GetErrorMessage()
                   << " code:" << ex.GetError().code;

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    })
    .onError([fileId](const std::system_error &ex) mutable {
        LOG(ERROR) << "Creation of file " << fileId
                   << " failed due to: " << ex.what();

        return makeFuturePosixException<folly::Unit>(EIO);
    });
}

folly::Future<folly::Unit> XRootDHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    return mkdir(fileId, mode, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> mkdirTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
            });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::MkDir(m_fs, ensureAbsPath(url().GetPath(), fileId.toStdString()),
        XrdCl::MkDirFlags::None, modeToAccess(mode)) >>
    mkdirTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        fileId, mode, retryCount,
        s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            LOG(WARNING) << "Retrying mkdir " << fileId << " due to "
                         << ex.GetError().GetErrorMessage();

            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.mkdir.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([
                    self = std::move(self), fileId, mode, retryCount
                ]() mutable {
                    return self->mkdir(fileId, mode, retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::Unit> XRootDHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return rename(from, to, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::rename(const folly::fbstring &from,
    const folly::fbstring &to, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> mvTask{
        [p = std::move(p)](XrdCl::XRootDStatus & st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::Mv(m_fs, ensureAbsPath(url().GetPath(), from.toStdString()),
        ensureAbsPath(url().GetPath(), to.toStdString())) >>
    mvTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
    .onError([
        from, to, retryCount,
        s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        LOG(ERROR) << "Mv failed due to: " << ex.GetError().GetErrorMessage();

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.mkdir.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then(
                    [ self = std::move(self), from, to, retryCount ]() mutable {
                        return self->rename(from, to, retryCount - 1);
                    });
        }

        return makeFuturePosixException<folly::Unit>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    return readdir(fileId, offset, count, kXRootDRetryCount);
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count,
    const int retryCount)
{
    auto p = folly::Promise<folly::fbvector<folly::fbstring>>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus &, XrdCl::DirectoryList &)>
        dirlistTask{[p = std::move(p), offset, count](
            XrdCl::XRootDStatus & st, XrdCl::DirectoryList & dirList) mutable {
            p.setWith([&st, &dirList, offset, count]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                folly::fbvector<folly::fbstring> result{};

                auto size = dirList.GetSize();

                if (offset > size)
                    return result;

                auto it = dirList.Begin();
                std::advance(it, std::min<std::size_t>(offset, size));
                for (; (it != dirList.End()) && (result.size() < count); ++it) {
                    result.emplace_back((*it)->GetName());
                }

                return result;
            });
}
}
;

auto tf = XrdCl::Async(
    XrdCl::DirList(m_fs, ensureAbsPath(url().GetPath(), fileId.toStdString()),
        XrdCl::DirListFlags::None) >>
    dirlistTask);

return f
    .then(executor().get(),
        [tf = std::move(tf)](auto &&result) { return result; })
    .onError([
        fileId, offset, count, retryCount,
        s = std::weak_ptr<XRootDHelper>{shared_from_this()}
    ](const XrdCl::PipelineException &ex) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                ECANCELED);

        if (retryCount > 0 && shouldRetryError(ex)) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.mkdir.retries")
            return folly::via(self->executor().get())
                .delayed(retryDelay(retryCount))
                .then([
                    self = std::move(self), fileId, offset, count, retryCount
                ]() mutable {
                    return self->readdir(fileId, offset, count, retryCount - 1);
                });
        }

        return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
            xrootdStatusToPosixError(ex.GetError()));
    });
}
} // namespace helpers
} // namespace one
