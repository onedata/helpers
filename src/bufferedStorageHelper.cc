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

#include <utility>

namespace one {
namespace helpers {

BufferedStorageFileHandle::BufferedStorageFileHandle(folly::fbstring fileId,
    std::shared_ptr<BufferedStorageHelper> helper,
    FileHandlePtr bufferStorageHandle, FileHandlePtr mainStorageHandle)
    : FileHandle{std::move(fileId), helper}
    , m_bufferStorageHelper{helper->bufferHelper()}
    , m_mainStorageHelper{helper->mainHelper()}
    , m_bufferStorageHandle{std::move(bufferStorageHandle)}
    , m_mainStorageHandle{std::move(mainStorageHandle)}
{
    LOG_FCALL() << LOG_FARG(helper->blockSize());
}

folly::Future<folly::IOBufQueue> BufferedStorageFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    return m_bufferStorageHandle->helper()
        ->getattr(m_bufferStorageHandle->fileId())
        .thenValue([this, offset, size](const auto && /*attr*/) {
            return m_bufferStorageHandle->read(offset, size);
        })
        .thenError(folly::tag_t<std::system_error>{},
            [this, offset, size](auto && /*e*/) {
                return m_mainStorageHandle->read(offset, size);
            });
}

folly::Future<std::size_t> BufferedStorageFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    return loadBufferBlocks(offset, buf.chainLength())
        .thenValue([this, offset, buf = std::move(buf),
                       writeCb = std::move(writeCb)](auto && /*unit*/) mutable {
            return m_bufferStorageHandle->write(
                offset, std::move(buf), std::move(writeCb));
        });
}

folly::Future<folly::Unit> BufferedStorageFileHandle::release()
{
    LOG_FCALL();

    return m_bufferStorageHandle->release();
}

folly::Future<folly::Unit> BufferedStorageFileHandle::flush()
{
    LOG_FCALL();

    return m_bufferStorageHandle->flush();
}

folly::Future<folly::Unit> BufferedStorageFileHandle::fsync(bool isDataSync)
{
    LOG_FCALL();

    return m_bufferStorageHandle->fsync(isDataSync);
}

folly::Future<folly::Unit> BufferedStorageFileHandle::loadBufferBlocks(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

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
                .thenValue([](auto && /*attr*/) {
                    // Ignore blocks not existent on main storage
                    return folly::makeFuture<std::size_t>(0);
                })
                .thenError(folly::tag_t<std::system_error>{},
                    [this, blockOffset, blockSize](auto && /*e*/) {
                        return m_mainStorageHandle->read(blockOffset, blockSize)
                            .thenValue(
                                [this, blockOffset](folly::IOBufQueue &&buf) {
                                    return m_bufferStorageHandle->write(
                                        blockOffset, std::move(buf), {});
                                })
                            .thenError(folly::tag_t<std::system_error>{},
                                [](auto && /*e*/) {
                                    // Ignore blocks not existent on main
                                    // storage
                                    return folly::makeFuture<std::size_t>(0);
                                });
                    });

        futs.emplace_back(std::move(fut));

        blockNumber++;
        blockOffset = blockNumber * blockSize;
    }

    if (futs.empty())
        return folly::makeFuture();

    auto *executor = m_bufferStorageHelper->executor().get();

    return folly::collectAll(futs.begin(), futs.end())
        .via(executor)
        .thenValue([](const std::vector<folly::Try<std::size_t>> &res) {
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

    auto *executor = m_bufferStorage->executor().get();

    return folly::collectAll(futs).via(executor).thenValue(
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
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    if (m_bufferStorage->storagePathType() == StoragePathType::FLAT &&
        m_mainStorage->storagePathType() == StoragePathType::CANONICAL) {
        // Move the objects from the buffer to the canonical storage
        // using multipartCopy, depending on the state of the file in the
        // main storage
        return m_mainStorage->getattr(fileId)
            .thenValue([](auto && /*unit*/) { return folly::makeFuture(); })
            .thenError(folly::tag_t<std::system_error>{},
                [this, fileId, size](auto && /*e*/) {
                    return std::dynamic_pointer_cast<KeyValueAdapter>(
                        m_bufferStorage)
                        ->fillMissingFileBlocks(toBufferPath(fileId), size);
                })
            .thenValue([this, fileId, size](auto && /*unit*/) {
                return m_bufferStorage
                    ->multipartCopy(toBufferPath(fileId), fileId,
                        m_bufferStorage->blockSize(), size)
                    .thenValue([this, fileId, size](auto && /*unit*/) {
                        return m_bufferStorage->unlink(
                            toBufferPath(fileId), size);
                    });
            });
    }
    return {};
}

folly::Future<folly::Unit> BufferedStorageHelper::checkStorageAvailability()
{
    LOG_FCALL();

    return applyAsync<folly::Unit>(m_mainStorage->checkStorageAvailability(),
        m_bufferStorage->checkStorageAvailability())
        .thenTry([](folly::Try<std::pair<folly::Unit, folly::Unit>> &&maybe) {
            if (maybe.hasException())
                maybe.throwIfFailed();

            return folly::makeFuture();
        });
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
        .thenValue([this, fileId, currentSize](auto && /*unit*/) {
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
            .thenError(folly::tag_t<std::system_error>{},
                [](auto && /*e*/) { return folly::makeFuture(); })
            .thenValue([this, fileId, currentSize](auto && /*unit*/) {
                return m_mainStorage->unlink(fileId, currentSize)
                    .thenValue([this, fileId](auto && /*unit*/) {
                        const auto kDefaultMode = 0644;
                        return m_mainStorage->mknod(
                            fileId, kDefaultMode, maskToFlags(S_IFREG), 0);
                    });
            });
    }

    return m_mainStorage->truncate(fileId, size, currentSize)
        .thenError(folly::tag_t<std::system_error>{},
            [this, fileId, size, currentSize](auto &&e) {
                if (e.code().value() != ENOENT)
                    throw e;

                return m_bufferStorage
                    ->truncate(toBufferPath(fileId), size, currentSize)
                    .thenValue([this, fileId, size](auto && /*unit*/) {
                        return flushBuffer(fileId, size);
                    });
            });
}

folly::Future<FileHandlePtr> BufferedStorageHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return applyAsync<FileHandlePtr>(
        m_bufferStorage->open(toBufferPath(fileId), flags, openParams),
        m_mainStorage->open(fileId, flags, openParams))
        .thenTry(
            [this, fileId](folly::Try<std::pair<FileHandlePtr, FileHandlePtr>>
                    &&maybeHandles) {
                if (maybeHandles.hasException())
                    maybeHandles.throwIfFailed();

                LOG_DBG(3) << "Got buffered storage handles";

                return std::make_shared<BufferedStorageFileHandle>(fileId,
                    shared_from_this(),
                    std::move(std::get<0>(maybeHandles.value())),
                    std::move(std::get<1>(maybeHandles.value())));
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

folly::Future<std::size_t> BufferedStorageHelper::blockSizeForPath(
    const folly::fbstring &fileId)
{
    return m_bufferStorage->blockSizeForPath(fileId);
}

std::size_t BufferedStorageHelper::blockSize() const
{
    return m_bufferStorage->blockSize();
}

const Timeout &BufferedStorageHelper::timeout()
{
    return m_bufferStorage->timeout();
}

StoragePathType BufferedStorageHelper::storagePathType() const
{
    return m_bufferStorage->storagePathType();
}

bool BufferedStorageHelper::isObjectStorage() const
{
    return m_bufferStorage->isObjectStorage();
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

std::shared_ptr<folly::Executor> BufferedStorageHelper::executor()
{
    return m_mainStorage->executor();
}
} // namespace helpers
} // namespace one
