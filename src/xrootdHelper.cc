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

namespace one {
namespace helpers {

XRootDFileHandle::XRootDFileHandle(
    folly::fbstring fileId, std::shared_ptr<XRootDHelper> helper)
    : FileHandle{fileId, std::move(helper)}
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
    assert(m_file.IsOpen());

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
                return info.length;
            });
        }};

    XrdCl::Async(XrdCl::Read(m_file, offset, size, data) >> readTask);

    return f.then(helper()->executor().get(),
        [buf = std::move(buf), offset, size](std::size_t readBytes) mutable {
            buf.postallocate(static_cast<std::size_t>(readBytes));
            return folly::makeFuture(std::move(buf));
        });
}

folly::Future<std::size_t> XRootDFileHandle::write(
    const off_t offset, folly::IOBufQueue buf)
{
    return write(offset, std::move(buf), kXRootDRetryCount);
}

folly::Future<std::size_t> XRootDFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, const int retryCount)
{
    assert(m_file.IsOpen());

    auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();
    if (iobuf->isChained()) {
        iobuf->unshare();
        iobuf->coalesce();
    }

    auto data = reinterpret_cast<void*>(iobuf->writableData());
    auto size = iobuf->length();

    // Create a template wrapper for this //////
    auto p = folly::Promise<folly::Unit>();
    auto f = p.getFuture();

    std::packaged_task<void(XrdCl::XRootDStatus & st)>
        writeTask{[buf = std::move(buf), p = std::move(p)](
                     XrdCl::XRootDStatus &st) mutable {
            p.setWith([&st]() {
                if (!st.IsOK())
                    throw XrdCl::PipelineException(st);
            });
        }};

    XrdCl::Async(XrdCl::Write(m_file, offset, size, data) >> writeTask);

    return f.then(helper()->executor().get(),
        [size]() mutable { return folly::makeFuture(size); });

    /////////////////////////
}

const Timeout &XRootDFileHandle::timeout() { return m_helper->timeout(); }

XRootDHelper::XRootDHelper(std::shared_ptr<XRootDHelperParams> params)
{
    invalidateParams()->setValue(std::move(params));
}

/**
 * Destructor.
 * Closes connection to XRootD storage cluster and destroys internal
 * context object.
 */
~XRootDHelper::XRootDHelper() {}

folly::fbstring name() const override { return XROOTD_HELPER_NAME; };

folly::Future<folly::Unit> XRootDHelper::access(
    const folly::fbstring &fileId, const int mask)
{
}

folly::Future<folly::Unit> XRootDHelper::access(
    const folly::fbstring &fileId, const int mask, const int retryCount)
{
}

folly::Future<struct stat> XRootDHelper::getattr(const folly::fbstring &fileId)
{
}

folly::Future<struct stat> XRootDHelper::getattr(
    const folly::fbstring &fileId, const int retryCount)
{
}

folly::Future<FileHandlePtr> XRootDHelper::open(
    const folly::fbstring &fileId, const int, const Params &)
{
}

folly::Future<folly::Unit> XRootDHelper::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
}

folly::Future<folly::Unit> XRootDHelper::unlink(const folly::fbstring &fileId,
    const size_t currentSize, const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::rmdir(const folly::fbstring &fileId) {}

folly::Future<folly::Unit> XRootDHelper::rmdir(
    const folly::fbstring &fileId, const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
}

folly::Future<folly::Unit> XRootDHelper::truncate(const folly::fbstring &fileId,
    const off_t size, const size_t currentSize, const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet &flags, const dev_t rdev)
{
}

folly::Future<folly::Unit> XRootDHelper::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet &flags, const dev_t rdev,
    const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
}

folly::Future<folly::Unit> XRootDHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode, const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
}

folly::Future<folly::Unit> XRootDHelper::rename(const folly::fbstring &from,
    const folly::fbstring &to, const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode) override
{
    return folly::makeFuture();
}

folly::Future<folly::Unit> XRootDHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid) override
{
    return folly::makeFuture();
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count,
    const int retryCount)
{
}

folly::Future<folly::fbstring> XRootDHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
}

folly::Future<folly::fbstring> XRootDHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace)
{
}

folly::Future<folly::Unit> XRootDHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool create,
    bool replace, const int retryCount)
{
}

folly::Future<folly::Unit> XRootDHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
}

folly::Future<folly::Unit> XRootDHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const int retryCount)
{
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::listxattr(
    const folly::fbstring &fileId)
{
}

folly::Future<folly::fbvector<folly::fbstring>> XRootDHelper::listxattr(
    const folly::fbstring &fileId, const int retryCount)
{
}

} // namespace helpers
} // namespace one

