/**
 * @file BufferedStorageHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "bufferedStorageHelper.h"
#include "keyValueAdapter.h"
#include "keyValueHelper.h"

#include <folly/Range.h>
#include <folly/String.h>

namespace one {
namespace helpers {

BufferedStorageFileHandle::BufferedStorageFileHandle(folly::fbstring fileId,
    std::shared_ptr<BufferedStorageHelper> helper,
    FileHandlePtr bufferStorageHandle, FileHandlePtr mainStorageHandle)
    : FileHandle{fileId, helper}
    , m_bufferStorageHelper{helper->bufferHelper()}
    , m_mainStorageHelper{helper->mainHelper()}
    , m_bufferStorageHandle{std::move(bufferStorageHandle)}
    , m_mainStorageHandle{std::move(mainStorageHandle)}
{
}

folly::Future<folly::IOBufQueue> BufferedStorageFileHandle::read(
    const off_t offset, const std::size_t size)
{
    return m_bufferStorageHandle->helper()
        ->getattr(m_bufferStorageHandle->fileId())
        .then([this, offset, size](const auto && /*attr*/) {
            return m_bufferStorageHandle->read(offset, size);
        })
        .onError([this, offset, size](const std::system_error & /*e*/) {
            return m_mainStorageHandle->read(offset, size);
        });
}

folly::Future<std::size_t> BufferedStorageFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    return loadBufferBlocks(offset, buf.chainLength())
        .then([this, offset, buf = std::move(buf),
                  writeCb = std::move(writeCb)]() mutable {
            return m_bufferStorageHandle->write(
                offset, std::move(buf), std::move(writeCb));
        });
}

folly::Future<folly::Unit> BufferedStorageFileHandle::release()
{
    return m_bufferStorageHandle->release();
}

folly::Future<folly::Unit> BufferedStorageFileHandle::flush()
{
    return m_bufferStorageHandle->flush();
}

folly::Future<folly::Unit> BufferedStorageFileHandle::fsync(bool isDataSync)
{
    return m_bufferStorageHandle->fsync(isDataSync);
}

folly::Future<folly::Unit> BufferedStorageFileHandle::loadBufferBlocks(
    const off_t offset, const std::size_t size)
{
    // Load blocks coincident with the offset+size from the main storage
    // to the buffer storage
    const auto blockSize = m_bufferStorageHelper->blockSize();

    auto bufferedStorageHelper =
        std::dynamic_pointer_cast<BufferedStorageHelper>(helper());

    std::vector<folly::fbstring> coincidentBlockIds;
    std::size_t blockNumber = std::floor(offset / blockSize);
    std::size_t blockOffset = blockNumber * blockSize;

    std::vector<folly::Future<std::size_t>> futs;

    while (blockOffset < offset + size) {
        folly::fbstring blockFileId{bufferedStorageHelper->toBufferPath(
            fmt::format("{}/{}", fileId(), MAX_OBJECT_ID - blockNumber))};

        LOG_DBG(2) << "Loading block to buffer: " << blockFileId;

        // If the block is not in the buffer, recall it asynchronously from the
        // main storage
        auto fut =
            m_bufferStorageHelper->getattr(blockFileId)
                .then([](auto && /*attr*/) {
                    // Ignore blocks not existent on main storage
                    return folly::makeFuture<std::size_t>(0);
                })
                .onError([this, blockOffset, blockSize](
                             const std::system_error & /*e*/) {
                    return m_mainStorageHandle->read(blockOffset, blockSize)
                        .then([this, blockOffset](folly::IOBufQueue &&buf) {
                            return m_bufferStorageHandle->write(
                                blockOffset, std::move(buf), {});
                        })
                        .onError([](const std::system_error & /*e*/) {
                            // Ignore blocks not existent on main storage
                            return folly::makeFuture<std::size_t>(0);
                        });
                });

        futs.emplace_back(std::move(fut));

        blockNumber++;
        blockOffset = blockNumber * blockSize;
    }

    return folly::collectAll(futs.begin(), futs.end())
        .then([](const std::vector<folly::Try<std::size_t>> &res) {
            std::for_each(res.begin(), res.end(),
                [](const auto &v) { v.throwIfFailed(); });
            return folly::makeFuture();
        });
}

BufferedStorageHelper::BufferedStorageHelper(StorageHelperPtr bufferStorage,
    StorageHelperPtr mainStorage, ExecutionContext executionContext,
    folly::fbstring bufferPath, const int bufferDepth,
    const std::size_t /*bufferStorageSize*/)
    : StorageHelper{executionContext}
    , m_bufferStorage{std::move(bufferStorage)}
    , m_mainStorage{std::move(mainStorage)}
    , m_bufferPath{std::move(bufferPath)}
    , m_bufferDepth{bufferDepth}
{
}

template <typename T, typename F>
folly::Future<std::pair<T, T>> BufferedStorageHelper::applyAsync(
    F &&bufferOp, F &&mainOp, bool ignoreBufferError)
{
    std::vector<folly::Future<T>> futs;
    futs.emplace_back(std::forward<F>(bufferOp));
    futs.emplace_back(std::forward<F>(mainOp));

    return folly::collectAll(futs).then(
        [ignoreBufferError](std::vector<folly::Try<T>> &&res) {
            if (!ignoreBufferError)
                res[0].throwIfFailed();
            res[1].throwIfFailed();

            return std::make_pair<T, T>(
                std::move(res[0].value()), std::move(res[1].value()));
        });
}

folly::fbstring BufferedStorageHelper::name() const
{
    return BUFFERED_STORAGE_HELPER_NAME;
}

folly::Future<folly::Unit> BufferedStorageHelper::loadBuffer(
    const folly::fbstring & /*fileId*/, const std::size_t /*size*/)
{
    return {};
}

folly::Future<folly::Unit> BufferedStorageHelper::flushBuffer(
    const folly::fbstring &fileId, const std::size_t size)
{
    if (m_bufferStorage->storagePathType() == StoragePathType::FLAT &&
        m_mainStorage->storagePathType() == StoragePathType::CANONICAL) {
        // Move the objects from the buffer to the canonical storage
        // using multipartCopy, depending on the state of the file in the
        // main storage
        return m_mainStorage->getattr(fileId)
            .then([]() { return folly::makeFuture(); })
            .onError([this, fileId, size](const std::system_error & /*e*/) {
                return std::dynamic_pointer_cast<KeyValueAdapter>(
                    m_bufferStorage)
                    ->fillMissingFileBlocks(toBufferPath(fileId), size);
            })
            .then([this, fileId, size]() {
                return m_bufferStorage
                    ->multipartCopy(toBufferPath(fileId), fileId,
                        m_bufferStorage->blockSize(), size)
                    .then([this, fileId, size]() {
                        return m_bufferStorage->unlink(
                            toBufferPath(fileId), size);
                    });
            });
    }
    return {};
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
    return m_bufferStorage->unlink(toBufferPath(fileId), currentSize)
        .then([this, fileId, currentSize]() {
            return m_mainStorage->unlink(fileId, currentSize);
        });
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
    if (size == 0) {
        return m_bufferStorage->unlink(toBufferPath(fileId), currentSize)
            .onError([](const std::system_error & /*e*/) {
                return folly::makeFuture();
            })
            .then([this, fileId, currentSize]() {
                return m_mainStorage->unlink(fileId, currentSize)
                    .then([this, fileId]() {
                        const auto kDefaultMode = 0644;
                        return m_mainStorage->mknod(
                            fileId, kDefaultMode, maskToFlags(S_IFREG), 0);
                    });
            });
    }

    return m_mainStorage->truncate(fileId, size, currentSize)
        .onError([this, fileId, size, currentSize](const std::system_error &e) {
            if (e.code().value() != ENOENT)
                throw e;

            return m_bufferStorage
                ->truncate(toBufferPath(fileId), size, currentSize)
                .then([this, fileId, size]() {
                    return flushBuffer(fileId, size);
                });
        });
}

folly::Future<FileHandlePtr> BufferedStorageHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    return applyAsync<FileHandlePtr>(
        m_bufferStorage->open(toBufferPath(fileId), flags, openParams),
        m_mainStorage->open(fileId, flags, openParams))
        .then(
            [this, fileId](std::pair<FileHandlePtr, FileHandlePtr> &&handles) {
                return std::make_shared<BufferedStorageFileHandle>(fileId,
                    shared_from_this(), std::move(std::get<0>(handles)),
                    std::move(std::get<1>(handles)));
            });
}

folly::Future<ListObjectsResult> BufferedStorageHelper::listobjects(
    const folly::fbstring &prefix, const folly::fbstring &marker,
    const off_t offset, const size_t count)
{
    return m_mainStorage->listobjects(prefix, marker, offset, count);
}

folly::Future<folly::Unit> BufferedStorageHelper::multipartCopy(
    const folly::fbstring & /*sourceKey*/,
    const folly::fbstring & /*destinationKey*/, const std::size_t /*blockSize*/,
    const std::size_t /*size*/)
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

folly::fbstring BufferedStorageHelper::toBufferPath(
    const folly::fbstring &fileId)
{
    folly::fbstring result{};
    std::vector<std::string> fileIdTokensVec;
    folly::split("/", fileId, fileIdTokensVec, false);
    std::list<folly::fbstring> fileIdTokens{
        fileIdTokensVec.begin(), fileIdTokensVec.end()};
    if (!m_bufferPath.empty()) {
        auto it = fileIdTokens.begin();
        std::advance(it, m_bufferDepth + 1);
        fileIdTokens.insert(it, m_bufferPath);
    }
    folly::join("/", fileIdTokens.begin(), fileIdTokens.end(), result);

    return result;
}
} // namespace helpers
} // namespace one