/**
 * @file BufferedStorageHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "bufferedStorageHelper.h"

namespace one {
namespace helpers {

BufferedStorageFileHandle::BufferedStorageFileHandle(
    folly::fbstring fileId, std::shared_ptr<BufferedStorageHelper> helper)
    : m_buffer{helper->bufferHelper()}
    , m_main{helper->mainStorage()}
{
}

folly::Future<folly::IOBufQueue> BufferedStorageFileHandle::read(
    const off_t offset, const std::size_t size)
{
}

folly::Future<std::size_t> BufferedStorageFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
}

folly::Future<folly::Unit> BufferedStorageFileHandle::release() {}

folly::Future<folly::Unit> BufferedStorageFileHandle::flush() {}

folly::Future<folly::Unit> BufferedStorageFileHandle::fsync(bool isDataSync) {}

BufferedStorageHelper::BufferedStorageHelper(StorageHelperPtr bufferStorage,
    StorageHelperPtr mainStorage, const std::size_t bufferStorageSize,
    folly::fbstring cachePrefix, ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_routes{std::move(routes)}
{
}

folly::fbstring BufferedStorageHelper::name() const
{
    return BUFFERED_STORAGE_HELPER_NAME;
}

folly::Future<struct stat> BufferedStorageHelper::getattr(
    const folly::fbstring &fileId)
{
    return m_mainStorage->getattr(fileId);
}

folly::Future<folly::Unit> BufferedStorageHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    return m_mainStorage->access(fileId, mask);
}

folly::Future<folly::fbstring> BufferedStorageHelper::readlink(
    const folly::fbstring &fileId)
{
    return m_mainStorage->readlink(fileId);
}

folly::Future<folly::fbvector<folly::fbstring>> BufferedStorageHelper::readdir(
    const folly::fbstring &fileId, const off_t offset, const std::size_t count)
{
    return m_mainStorage->readdir(fileId, offset, count);
}

folly::Future<folly::Unit> BufferedStorageHelper::mknod(
    const folly::fbstring &fileId, const mode_t mode, const FlagsSet &flags,
    const dev_t rdev)
{
    return m_mainStorage->mknod(fileId, mode, flags, rdev);
}

folly::Future<folly::Unit> BufferedStorageHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    return m_mainStorage->mkdir(fileId, mode);
}

folly::Future<folly::Unit> BufferedStorageHelper::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    return m_mainStorage->unlink(fileId, currentSize);
}

folly::Future<folly::Unit> BufferedStorageHelper::rmdir(
    const folly::fbstring &fileId)
{
    return m_mainStorage->rmdir(fileId);
}

folly::Future<folly::Unit> BufferedStorageHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return m_mainStorage->symlink(from, to);
}

folly::Future<folly::Unit> BufferedStorageHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return m_mainStorage->rename(from, to);
}

folly::Future<folly::Unit> BufferedStorageHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return m_mainStorage->link(from, to);
}

folly::Future<folly::Unit> BufferedStorageHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    return m_mainStorage->chmod(fileId, mode);
}

folly::Future<folly::Unit> BufferedStorageHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    return m_mainStorage->chown(fileId, uid, gid);
}

folly::Future<folly::Unit> BufferedStorageHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    return m_mainStorage->chown(fileId, size, currentSize);
}

folly::Future<FileHandlePtr> BufferedStorageHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    return m_mainStorage->open(fileId, flags, openParams);
}

folly::Future<ListObjectsResult> BufferedStorageHelper::listobjects(
    const folly::fbstring &prefix, const folly::fbstring &marker,
    const off_t offset, const size_t count)
{
    // TODO handle case when prefix matches multiple routes
    return m_mainStorage->listobjects(prefix, marker, offset, count);
}

folly::Future<folly::Unit> BufferedStorageHelper::multipartCopy(
    const folly::fbstring & /*sourceKey*/,
    const folly::fbstring & /*destinationKey*/)
{
    throw std::system_error{
        std::make_error_code(std::errc::function_not_supported)};
}

folly::Future<folly::fbstring> BufferedStorageHelper::getxattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    return m_mainStorage->getxattr(uuid, name);
}

folly::Future<folly::Unit> BufferedStorageHelper::setxattr(
    const folly::fbstring &uuid, const folly::fbstring &name,
    const folly::fbstring &value, bool create, bool replace)
{
    return m_mainStorage->setxattr(uuid, name, value, create, replace);
}

folly::Future<folly::Unit> BufferedStorageHelper::removexattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    return m_mainStorage->removexattr(uuid, name);
}

folly::Future<folly::fbvector<folly::fbstring>>
BufferedStorageHelper::listxattr(const folly::fbstring &uuid)
{
    return m_mainStorage->listxattr(uuid);
}
} // namespace helpers
} // namespace one
