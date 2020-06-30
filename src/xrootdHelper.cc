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

#include <XrdCl/XrdClFileOperations.hh>
#include <XrdCl/XrdClFileSystemOperations.hh>

namespace one {
namespace helpers {

namespace {
/*
 *
 *const std::set<int> WebDAV_RETRY_ERRORS = {EINTR, EIO, EAGAIN, EACCES, EBUSY,
 *    EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK, EWOULDBLOCK, ENONET,
 *    ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN, ENETUNREACH, ECONNABORTED,
 *    ECONNRESET, ENOTCONN, EHOSTDOWN, EHOSTUNREACH, EREMOTEIO, ENOMEDIUM,
 *    ECANCELED};
 *
 *inline bool shouldRetryError(int ec)
 *{
 *    return WebDAV_RETRY_ERRORS.find(ec) != WebDAV_RETRY_ERRORS.cend();
 *}
 */

const auto kXRootDRetryMinimumDelay = std::chrono::milliseconds{5};

inline auto retryDelay(int retriesLeft)
{
    const unsigned int kXRootDRetryBaseDelay_ms = 100;
    return kXRootDRetryMinimumDelay +
        std::chrono::milliseconds{kXRootDRetryBaseDelay_ms *
            (kXRootDRetryCount - retriesLeft) *
            (kXRootDRetryCount - retriesLeft)};
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
    assert(m_file->IsOpen());

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
    char *data = static_cast<char *>(buf.preallocate(size, size).first);

    auto p = folly::Promise<std::size_t>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st, XrdCl::ChunkInfo & info)>
        readTask{[p = std::move(p)](
                     XrdCl::XRootDStatus &st, XrdCl::ChunkInfo &info) mutable {
            p.setWith([&st, &info]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return static_cast<std::size_t>(info.length);
            });
        }};

    auto tf =
        XrdCl::Async(XrdCl::Read(m_file.get(), offset, size, data) >> readTask);

    return f
        .then(helper()->executor().get(),
            [buf = std::move(buf), offset, size, tf = std::move(tf)](
                std::size_t readBytes) mutable {
                buf.postallocate(static_cast<std::size_t>(readBytes));
                return folly::makeFuture(std::move(buf));
            })
        .onError([retryCount, offset, size,
                     s = std::weak_ptr<XRootDFileHandle>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::IOBufQueue>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.read.retries")
                return folly::via(self->helper()->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), size, offset,
                              retryCount]() mutable {
                        return self->read(offset, size, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::IOBufQueue>(EIO);
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
    assert(m_file->IsOpen());

    auto data = reinterpret_cast<const void *>(buf.front()->data());
    auto size = buf.front()->length();

    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> writeTask{
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::Write(m_file.get(), static_cast<long unsigned int>(offset),
            static_cast<unsigned int>(size), const_cast<void *>(data)) >>
        writeTask);

    return f
        .then(helper()->executor().get(),
            [size, tf = std::move(tf)]() mutable {
                return folly::makeFuture(size);
            })
        .onError([retryCount, offset, buf = std::move(buf),
                     s = std::weak_ptr<XRootDFileHandle>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<std::size_t>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.access.retries")
                return folly::via(self->helper()->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), buf = std::move(buf), offset,
                              retryCount]() mutable {
                        return self->write(
                            offset, std::move(buf), retryCount - 1);
                    });
            }

            return makeFuturePosixException<std::size_t>(EIO);
        });
}

folly::Future<folly::Unit> XRootDFileHandle::release() {}

folly::Future<folly::Unit> XRootDFileHandle::fsync(bool isDataSync) {}

const Timeout &XRootDFileHandle::timeout() { return m_helper->timeout(); }

XRootDHelper::XRootDHelper(std::shared_ptr<XRootDHelperParams> params,
    std::shared_ptr<folly::IOExecutor> executor)
    : m_executor{std::move(executor)}
    , m_fs{params->url()}
{
    invalidateParams()->setValue(std::move(params));
}

/**
 * Destructor.
 * Closes connection to XRootD storage cluster and destroys internal
 * context object.
 */
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

    std::packaged_task<void(XrdCl::XRootDStatus & st, XrdCl::StatInfo & info)>
        statTask{[p = std::move(p)](
                     XrdCl::XRootDStatus &st, XrdCl::StatInfo &info) mutable {
            p.setWith([&st, &info]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(XrdCl::Stat(m_fs, fileId.toStdString()) >> statTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
        .onError([fileId, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.read.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then(
                        [self = std::move(self), fileId, retryCount]() mutable {
                            return self->access(fileId, retryCount - 1);
                        });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
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
        statTask{[p = std::move(p)](
                     XrdCl::XRootDStatus &st, XrdCl::StatInfo &info) mutable {
            p.setWith([&st, &info]() {
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
        }};

    auto tf = XrdCl::Async(XrdCl::Stat(m_fs, fileId.toStdString()) >> statTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)](struct stat &&attr) mutable {
                return folly::makeFuture<struct stat>(std::move(attr));
            })
        .onError([fileId, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<struct stat>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC(
                    "comp.helpers.mod.xrootd.getattr.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then(
                        [self = std::move(self), fileId, retryCount]() mutable {
                            return self->getattr(fileId, retryCount - 1);
                        });
            }

            return makeFuturePosixException<struct stat>(EIO);
        });
}

folly::Future<FileHandlePtr> XRootDHelper::open(const folly::fbstring &fileId,
    const int flags, const Params & /*openParams*/)
{
    auto p = folly::Promise<std::shared_ptr<XRootDFileHandle>>();
    auto f = p.getFuture();

    auto file = std::make_unique<XrdCl::File>(false);
    auto filePtr = file.get();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> openTask{
        [this, p = std::move(p), fileId, file = std::move(file)](
            XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st, fileId, file = std::move(file),
                          s = std::weak_ptr<XRootDHelper>{
                              shared_from_this()}]() mutable {
                auto self = s.lock();
                if (!self)
                    return makeFuturePosixException<
                        std::shared_ptr<XRootDFileHandle>>(ECANCELED);

                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                return folly::makeFuture<std::shared_ptr<XRootDFileHandle>>(
                    std::make_shared<XRootDFileHandle>(
                        fileId, std::move(file), std::move(self)));
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::Open(filePtr, url().GetURL() + fileId.toStdString(), {}, {}) >>
        openTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)](
                std::shared_ptr<XRootDFileHandle> &&handle) mutable {
                return folly::makeFuture<FileHandlePtr>(std::move(handle));
            })
        .onError([fileId, s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<FileHandlePtr>(ECANCELED);

            return makeFuturePosixException<FileHandlePtr>(EIO);
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
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(XrdCl::Rm(m_fs, fileId.toStdString()) >> rmTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
        .onError([fileId, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.unlink.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then(
                        [self = std::move(self), fileId, retryCount]() mutable {
                            return self->unlink(fileId, retryCount - 1);
                        });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
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
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
        }};

    auto tf =
        XrdCl::Async(XrdCl::RmDir(m_fs, fileId.toStdString()) >> rmDirTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
        .onError([fileId, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.unlink.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then(
                        [self = std::move(self), fileId, retryCount]() mutable {
                            return self->rmdir(fileId, retryCount - 1);
                        });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
        });
}

folly::Future<folly::Unit> XRootDHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    return truncate(fileId, size, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t currentSize, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> truncateTask{
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::Truncate(m_fs, fileId.toStdString(), size) >> truncateTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
        .onError([fileId, retryCount, size, currentSize,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.unlink.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), fileId, size, currentSize,
                              retryCount]() mutable {
                        return self->truncate(
                            fileId, size, currentSize, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
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
    return folly::makeFuture();
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
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::MkDir(m_fs, fileId.toStdString(), XrdCl::MkDirFlags::None,
            XrdCl::Access::Mode::UR | XrdCl::Access::Mode::UW |
                XrdCl::Access::Mode::UX | XrdCl::Access::Mode::GR |
                XrdCl::Access::Mode::GW | XrdCl::Access::Mode::GX) >>
        mkdirTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
        .onError([fileId, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.mkdir.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then(
                        [self = std::move(self), fileId, retryCount]() mutable {
                            return self->mkdir(fileId, retryCount - 1);
                        });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
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
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::Mv(m_fs, from.toStdString(), to.toStdString()) >> mvTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() mutable { return folly::makeFuture(); })
        .onError([from, to, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.mkdir.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), from, to,
                              retryCount]() mutable {
                        return self->rename(from, to, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
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
        dirlistTask{[p = std::move(p), offset, count](XrdCl::XRootDStatus &st,
                        XrdCl::DirectoryList &dirList) mutable {
            p.setWith([&st, &dirList, offset, count]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
                folly::fbvector<folly::fbstring> result{};

                auto size = dirList.GetSize();
                if (offset > size)
                    return result;

                auto it = dirList.Begin();
                std::advance(it, std::min<std::size_t>(offset + count, size));
                for (; it != dirList.End(); ++it) {
                    result.emplace_back((*it)->GetName());
                }

                return result;
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::DirList(m_fs, fileId.toStdString(), {}) >> dirlistTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)](auto &&result) { return result; })
        .onError([fileId, offset, count, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC("comp.helpers.mod.xrootd.mkdir.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), fileId, offset, count,
                              retryCount]() mutable {
                        return self->readdir(
                            fileId, offset, count, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                EIO);
        });
}

folly::Future<folly::fbstring> XRootDHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    return getxattr(fileId, name, kXRootDRetryCount);
}

folly::Future<folly::fbstring> XRootDHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const int retryCount)
{
    auto p = folly::Promise<folly::fbstring>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st, std::string & xattr)>
        getxattrTask{[p = std::move(p)](
                         XrdCl::XRootDStatus &st, std::string &xattr) mutable {
            p.setWith([&st, &xattr]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                return folly::fbstring{xattr};
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::GetXAttr(m_fs, fileId.toStdString(), name.toStdString()) >>
        getxattrTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)](auto &&attr) mutable {
                return folly::makeFuture<folly::fbstring>(std::move(attr));
            })
        .onError([fileId, name, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::fbstring>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC(
                    "comp.helpers.mod.xrootd.getxattr.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), fileId, name,
                              retryCount]() mutable {
                        return self->getxattr(fileId, name, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::fbstring>(EIO);
        });
}

folly::Future<folly::Unit> XRootDHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace)
{
    return setxattr(fileId, name, value, create, replace, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace, const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> setxattrTask{
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(XrdCl::SetXAttr(m_fs, fileId.toStdString(),
                               name.toStdString(), value.toStdString()) >>
        setxattrTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() { return folly::makeFuture(); })
        .onError([fileId, name, value, create, replace, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC(
                    "comp.helpers.mod.xrootd.setxattr.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), fileId, name, value, create,
                              replace, retryCount]() mutable {
                        return self->setxattr(fileId, name, value, create,
                            replace, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
        });
}

folly::Future<folly::Unit> XRootDHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    return removexattr(fileId, name, kXRootDRetryCount);
}

folly::Future<folly::Unit> XRootDHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const int retryCount)
{
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)> delxattrTask{
        [p = std::move(p)](XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                return folly::Unit();
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::DelXAttr(m_fs, fileId.toStdString(), name.toStdString()) >>
        delxattrTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)]() { return folly::makeFuture(); })
        .onError([fileId, name, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC(
                    "comp.helpers.mod.xrootd.removexattr.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then([self = std::move(self), fileId, name,
                              retryCount]() mutable {
                        return self->removexattr(fileId, name, retryCount - 1);
                    });
            }

            return makeFuturePosixException<folly::Unit>(EIO);
        });
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::listxattr(
    const folly::fbstring &fileId)
{
    return listxattr(fileId, kXRootDRetryCount);
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::listxattr(
    const folly::fbstring &fileId, const int retryCount)
{
    auto p = folly::Promise<folly::fbvector<folly::fbstring>>();
    auto f = p.getFuture();

    std::packaged_task<void(
        XrdCl::XRootDStatus & st, std::vector<XrdCl::XAttr> & xattrs)>
        listxattrTask{[p = std::move(p)](XrdCl::XRootDStatus &st,
                          std::vector<XrdCl::XAttr> &xattrs) mutable {
            p.setWith([&st, &xattrs]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);

                folly::fbvector<folly::fbstring> result{};
                for (const auto &xattr : xattrs)
                    result.emplace_back(xattr.name);

                return result;
            });
        }};

    auto tf = XrdCl::Async(
        XrdCl::ListXAttr(m_fs, fileId.toStdString()) >> listxattrTask);

    return f
        .then(executor().get(),
            [tf = std::move(tf)](auto &&attrs) mutable {
                return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                    std::move(attrs));
            })
        .onError([fileId, retryCount,
                     s = std::weak_ptr<XRootDHelper>{shared_from_this()}](
                     const XrdCl::PipelineException & /*unused*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(ECANCELED);

            if (retryCount > 0) {
                ONE_METRIC_COUNTER_INC(
                    "comp.helpers.mod.xrootd.listxattr.retries")
                return folly::via(self->executor().get())
                    .delayed(retryDelay(retryCount))
                    .then(
                        [self = std::move(self), fileId, retryCount]() mutable {
                            return self->listxattr(fileId, retryCount - 1);
                        });
            }

            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                EIO);
        });
}

} // namespace helpers
} // namespace one

