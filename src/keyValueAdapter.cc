/**
 * @file keyValueAdapter.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "keyValueAdapter.h"
#include "helpers/logging.h"
#include "keyValueHelper.h"

namespace {

uint64_t getBlockId(off_t offset, std::size_t blockSize)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(blockSize);

    if (blockSize == 0)
        return 0;

    return offset / blockSize;
}

off_t getBlockOffset(off_t offset, std::size_t blockSize)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(blockSize);

    if (blockSize == 0)
        return offset;

    return offset - getBlockId(offset, blockSize) * blockSize;
}

void logError(const folly::fbstring &operation, const std::system_error &error)
{
    LOG(ERROR) << "Operation '" << operation
               << "' failed due to: " << error.what()
               << " (code: " << error.code().value() << ")";
}

folly::IOBufQueue readBlock(
    const std::shared_ptr<one::helpers::KeyValueHelper> &helper,
    const folly::fbstring &key, const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(offset) << LOG_FARG(size);

    try {
        return helper->getObject(key, offset, size);
    }
    catch (const std::system_error &e) {
        if (e.code().value() == ENOENT)
            return folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};

        throw;
    }
}

folly::IOBufQueue fillToSize(folly::IOBufQueue buf, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(buf.chainLength()) << LOG_FARG(size);

    if (buf.chainLength() < size) {
        const std::size_t fillLength = size - buf.chainLength();
        auto data = static_cast<char *>(buf.allocate(fillLength));
        std::fill(data, data + fillLength, 0);
    }
    return buf;
}

} // namespace

namespace one {
namespace helpers {

KeyValueFileHandle::KeyValueFileHandle(folly::fbstring fileId,
    std::shared_ptr<KeyValueAdapter> helper, const std::size_t blockSize,
    std::shared_ptr<Locks> locks, std::shared_ptr<folly::Executor> executor)
    : FileHandle{std::move(fileId), std::move(helper)}
    , m_blockSize{blockSize}
    , m_locks{std::move(locks)}
    , m_executor{std::move(executor)}
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(blockSize);
}

folly::Future<folly::IOBufQueue> KeyValueFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
    return folly::via(m_executor.get(), [
        this, offset, size, locks = m_locks,
        helper = std::dynamic_pointer_cast<KeyValueAdapter>(m_helper)->helper(),
        self = shared_from_this()
    ] {
        // In case this is a storage with files stored in single objects,
        // read from an object using it's fileId as name of the object
        // on the storage
        if (m_blockSize == 0) {
            Locks::accessor acc;
            locks->insert(acc, m_fileId);
            auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

            auto res = folly::makeFuture<folly::IOBufQueue>(
                helper->getObject(m_fileId, offset, size));

            return res;
        }

        return readBlocks(offset, size);
    });
}

folly::Future<std::size_t> KeyValueFileHandle::write(
    const off_t offset, folly::IOBufQueue buf)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    if (buf.empty())
        return folly::makeFuture<std::size_t>(0);

    return folly::via(m_executor.get(), [
        this, offset, locks = m_locks,
        helper = std::dynamic_pointer_cast<KeyValueAdapter>(m_helper)->helper(),
        buf = std::move(buf), self = shared_from_this()
    ]() mutable {
        const auto size = buf.chainLength();
        if (size == 0 && offset > 0)
            return folly::makeFuture<std::size_t>(0);

        // In case this is a storage with files stored in single objects,
        // try to modify the contents in place
        if (m_blockSize == 0) {
            Locks::accessor acc;
            locks->insert(acc, m_fileId);
            auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

            if (size == 0) {
                folly::makeFuture<std::size_t>(static_cast<std::size_t>(
                    helper->modifyObject(m_fileId, std::move(buf), offset)));
            }
            else {
                folly::makeFuture<std::size_t>(static_cast<std::size_t>(
                    helper->modifyObject(m_fileId, std::move(buf), offset)));
            }

            return folly::makeFuture<std::size_t>(
                static_cast<std::size_t>(size));
        }

        auto blockId = getBlockId(offset, m_blockSize);
        auto blockOffset = getBlockOffset(offset, m_blockSize);

        folly::fbvector<folly::Future<folly::Unit>> writeFutures;

        for (std::size_t bufOffset = 0; bufOffset < size;
             blockOffset = 0, ++blockId) {

            const auto blockSize = std::min<std::size_t>(
                m_blockSize - blockOffset, size - bufOffset);

            auto writeFuture = via(m_executor.get(), [
                this, iobuf = buf.front()->clone(), blockId, blockOffset,
                bufOffset, blockSize, self = shared_from_this()
            ]() mutable {
                folly::IOBufQueue bufq{folly::IOBufQueue::cacheChainLength()};

                bufq.append(std::move(iobuf));
                bufq.trimStart(bufOffset);
                bufq.trimEnd(bufq.chainLength() - blockSize);

                return writeBlock(std::move(bufq), blockId, blockOffset);
            });

            writeFutures.emplace_back(std::move(writeFuture));

            bufOffset += blockSize;
        }

        return folly::collect(writeFutures)
            .then([size](const std::vector<folly::Unit> & /*unused*/) {
                return size;
            })
            .then([](folly::Try<std::size_t> t) {
                try {
                    return t.value();
                }
                catch (const std::system_error &e) {
                    logError("write", e);
                    throw;
                }
            });
    });
}

const Timeout &KeyValueFileHandle::timeout() { return m_helper->timeout(); }

KeyValueAdapter::KeyValueAdapter(std::shared_ptr<KeyValueHelper> helper,
    std::shared_ptr<folly::Executor> executor, std::size_t blockSize)
    : m_helper{std::move(helper)}
    , m_executor{std::move(executor)}
    , m_locks{std::make_shared<Locks>()}
    , m_blockSize{blockSize}
{
    LOG_FCALL() << LOG_FARG(blockSize);
}

folly::fbstring KeyValueAdapter::name() const { return m_helper->name(); }

folly::Future<folly::Unit> KeyValueAdapter::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId);

    if (currentSize == 0)
        return folly::makeFuture();

    return folly::via(m_executor.get(),
        [ fileId, helper = m_helper, blockSize = m_blockSize, currentSize ] {
            if (blockSize == 0) {
                helper->getObjectInfo(fileId);
                helper->deleteObjects({fileId});
            }
            else {
                try {
                    folly::fbvector<folly::fbstring> keysToDelete;
                    for (size_t objectId = 0;
                         objectId <= (currentSize / blockSize); objectId++) {
                        keysToDelete.emplace_back(
                            helper->getKey(fileId, objectId));
                    }
                    helper->deleteObjects(keysToDelete);
                }
                catch (const std::system_error &e) {
                    logError("unlink", e);
                    throw;
                }
            }
        });
}

folly::Future<folly::Unit> KeyValueAdapter::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet & /*flags*/, const dev_t /*rdev*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    if (m_blockSize > 0)
        return folly::makeFuture();

    if (!S_ISREG(mode)) {
        LOG_DBG(1)
            << "Ignoring mknod request for " << fileId << " with mode " << mode
            << " - only regular files can be created on object storages.";
        return folly::makeFuture();
    }

    return folly::via(
        m_executor.get(), [ fileId, helper = m_helper, locks = m_locks ] {
            Locks::accessor acc;
            locks->insert(acc, fileId);
            auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

            helper->putObject(fileId,
                folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()});

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> KeyValueAdapter::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    if (static_cast<size_t>(size) == currentSize)
        return folly::makeFuture();

    return folly::via(m_executor.get(), [
        fileId, size, currentSize, helper = m_helper, locks = m_locks,
        defBlockSize = m_blockSize
    ] {
        try {
            if (defBlockSize == 0) {
                Locks::accessor acc;
                locks->insert(acc, fileId);
                auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

                auto attr = helper->getObjectInfo(fileId);

                if (currentSize != static_cast<std::size_t>(attr.st_size)) {
                    LOG(WARNING) << "Current size in metadata of '" << fileId
                                 << "' is different than actual storage size ("
                                 << currentSize << "!=" << attr.st_size << ")";
                }

                if (size > attr.st_size) {
                    auto buf = fillToSize(
                        helper->getObject(fileId, 0, attr.st_size), size);
                    helper->putObject(fileId, std::move(buf));
                }
                else if (size < attr.st_size) {
                    if (size == 0) {
                        helper->putObject(fileId,
                            folly::IOBufQueue{
                                folly::IOBufQueue::cacheChainLength()});
                    }
                    else {
                        auto buf = helper->getObject(fileId, 0, size);
                        helper->putObject(fileId, std::move(buf));
                    }
                }
            }
            else {
                auto currentLastBlockId = getBlockId(currentSize, defBlockSize);
                auto newLastBlockId = getBlockId(size, defBlockSize);
                auto newLastBlockOffset = getBlockOffset(size, defBlockSize);

                // Generate a list of objects to delete
                folly::fbvector<folly::fbstring> keysToDelete;
                for (size_t objectId = 0; objectId <= currentLastBlockId;
                     objectId++) {
                    if ((objectId > newLastBlockId) ||
                        ((objectId == newLastBlockId) &&
                            (newLastBlockOffset == 0))) {
                        keysToDelete.emplace_back(
                            helper->getKey(fileId, objectId));
                    }
                }

                auto key = helper->getKey(fileId, newLastBlockId);
                auto remainderBlockSize =
                    static_cast<std::size_t>(newLastBlockOffset);

                if (remainderBlockSize == 0 && newLastBlockId > 0) {
                    key = helper->getKey(fileId, newLastBlockId - 1);
                    remainderBlockSize = defBlockSize;
                }

                if (remainderBlockSize > 0 || newLastBlockId > 0) {
                    Locks::accessor acc;
                    locks->insert(acc, key);
                    auto g =
                        folly::makeGuard([&]() mutable { locks->erase(acc); });

                    try {
                        auto buf = fillToSize(
                            readBlock(helper, key, 0, remainderBlockSize),
                            remainderBlockSize);
                        helper->putObject(key, std::move(buf));
                    }
                    catch (...) {
                        LOG(ERROR) << "Truncate failed due to unknown "
                                      "error during "
                                      "'fillToSize'";
                        throw;
                    }
                }

                if (!keysToDelete.empty())
                    helper->deleteObjects(keysToDelete);
            }
        }
        catch (const std::system_error &e) {
            logError("truncate", e);
            throw;
        }
    });
}

folly::Future<struct stat> KeyValueAdapter::getattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [ fileId, helper = m_helper, locks = m_locks ] {
            return helper->getObjectInfo(fileId);
        });
}

folly::Future<folly::Unit> KeyValueAdapter::access(
    const folly::fbstring &fileId, const int /*mask*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::via(
        m_executor.get(), [ fileId, helper = m_helper, locks = m_locks ] {
            auto res = helper->listObjects(fileId, "", 0, 1);

            if (res.empty())
                return makeFuturePosixException<folly::Unit>(ENOENT);

            return folly::makeFuture();
        });
}

folly::Future<folly::fbvector<folly::fbstring>> KeyValueAdapter::listobjects(
    const folly::fbstring &prefix, const folly::fbstring &marker,
    const off_t offset, const size_t count)
{
    LOG_FCALL() << LOG_FARG(prefix) << LOG_FARG(marker) << LOG_FARG(offset)
                << LOG_FARG(count);

    return folly::via(m_executor.get(),
        [ prefix, marker, offset, count, helper = m_helper, locks = m_locks ] {
            return helper->listObjects(prefix, marker, offset, count);
        });
}

const Timeout &KeyValueAdapter::timeout()
{
    LOG_FCALL();

    return m_helper->timeout();
}

folly::Future<folly::IOBufQueue> KeyValueFileHandle::readBlocks(
    const off_t offset, const std::size_t requestedSize)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(requestedSize);

    const auto size = requestedSize;

    if (size == 0)
        return folly::makeFuture(
            folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()});

    folly::fbvector<folly::Future<folly::IOBufQueue>> readFutures;

    auto blockId = getBlockId(offset, m_blockSize);
    auto blockOffset = getBlockOffset(offset, m_blockSize);
    for (std::size_t bufOffset = 0; bufOffset < size;
         blockOffset = 0, ++blockId) {

        const auto blockSize =
            std::min(static_cast<std::size_t>(m_blockSize - blockOffset),
                static_cast<std::size_t>(size - bufOffset));

        auto readFuture = via(m_executor.get(), [
            this, blockId, blockOffset, blockSize, self = shared_from_this()
        ] {
            return fillToSize(
                readBlock(blockId, blockOffset, blockSize), blockSize);
        });

        readFutures.emplace_back(std::move(readFuture));
        bufOffset += blockSize;
    }

    return folly::collect(readFutures)
        .then([](std::vector<folly::IOBufQueue> &&results) {
            folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
            for (auto &subBuf : results)
                buf.append(std::move(subBuf));

            return buf;
        })
        .then([](folly::Try<folly::IOBufQueue> &&t) {
            try {
                return std::move(t.value());
            }
            catch (const std::system_error &e) {
                logError("read", e);
                throw;
            }
        });
}

folly::IOBufQueue KeyValueFileHandle::readBlock(
    const uint64_t blockId, const off_t blockOffset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(blockId) << LOG_FARG(blockOffset) << LOG_FARG(size);

    auto helper =
        std::dynamic_pointer_cast<KeyValueAdapter>(m_helper)->helper();

    auto key = helper->getKey(m_fileId, blockId);

    Locks::accessor acc;
    m_locks->insert(acc, key);
    auto g = folly::makeGuard([&]() mutable { m_locks->erase(acc); });

    auto ret = ::readBlock(helper, key, blockOffset, size);
    return ret;
}

void KeyValueFileHandle::writeBlock(
    folly::IOBufQueue buf, const uint64_t blockId, const off_t blockOffset)
{
    LOG_FCALL() << LOG_FARG(buf.chainLength()) << LOG_FARG(blockId)
                << LOG_FARG(blockOffset);

    auto helper =
        std::dynamic_pointer_cast<KeyValueAdapter>(m_helper)->helper();

    auto key = helper->getKey(m_fileId, blockId);
    Locks::accessor acc;
    m_locks->insert(acc, key);
    auto g = folly::makeGuard([&]() mutable { m_locks->erase(acc); });

    if (buf.chainLength() != m_blockSize) {
        if (helper->hasRandomAccess()) {
            helper->putObject(key, std::move(buf), blockOffset);
        }
        else {
            auto fetchedBuf = ::readBlock(helper, key, 0, m_blockSize);

            folly::IOBufQueue filledBuf{folly::IOBufQueue::cacheChainLength()};

            if (blockOffset > 0) {
                if (!fetchedBuf.empty())
                    filledBuf.append(fetchedBuf.front()->clone());

                if (filledBuf.chainLength() >=
                    static_cast<std::size_t>(blockOffset))
                    filledBuf.trimEnd(filledBuf.chainLength() - blockOffset);

                filledBuf = fillToSize(std::move(filledBuf),
                    static_cast<std::size_t>(blockOffset));
            }

            filledBuf.append(std::move(buf));

            if (filledBuf.chainLength() < fetchedBuf.chainLength()) {
                fetchedBuf.trimStart(filledBuf.chainLength());
                filledBuf.append(std::move(fetchedBuf));
            }

            helper->putObject(key, std::move(filledBuf));
        }
    }
    else {
        helper->putObject(key, std::move(buf));
    }
}

folly::Future<FileHandlePtr> KeyValueAdapter::open(
    const folly::fbstring &fileId, const int /*flags*/,
    const Params &openParams)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGM(openParams);

    FileHandlePtr handle = std::make_shared<KeyValueFileHandle>(
        fileId, shared_from_this(), m_blockSize, m_locks, m_executor);

    return folly::makeFuture(std::move(handle));
}

} // namespace helpers
} // namespace one
