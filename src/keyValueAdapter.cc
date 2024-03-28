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
        auto *data = static_cast<char *>(buf.allocate(fillLength));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
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
    LOG_FCALL() << LOG_FARG(blockSize);
}

folly::Future<folly::IOBufQueue> KeyValueFileHandle::readFlat(
    const off_t offset, const std::size_t size,
    const std::size_t storageBlockSize)
{
    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    log_timer<> timer;

    return folly::via(m_executor.get(),
        [this, offset, size, storageBlockSize, locks = m_locks,
            helper = std::dynamic_pointer_cast<KeyValueAdapter>(this->helper())
                         ->helper(),
            timer, self = shared_from_this()]() {
            auto res = readBlocks(offset, size, storageBlockSize);

            log<read_write_perf>(fileId(), "KeyValueFileHandle", "read", offset,
                size, timer.stop());

            return res;
        });
}

folly::Future<folly::IOBufQueue> KeyValueFileHandle::readCanonical(
    const off_t offset, const std::size_t size)
{
    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    log_timer<> timer;

    return folly::via(m_executor.get(),
        [this, offset, size, locks = m_locks,
            helper = std::dynamic_pointer_cast<KeyValueAdapter>(this->helper())
                         ->helper(),
            timer, self = shared_from_this()]() {
            // If the file is in 1 object
            Locks::accessor acc;
            locks->insert(acc, fileId());
            auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

            auto res = folly::makeFuture<folly::IOBufQueue>(
                helper->getObject(fileId(), offset, size));

            log<read_write_perf>(fileId(), "KeyValueFileHandle", "read", offset,
                size, timer.stop());

            return res;
        });
}

folly::Future<folly::IOBufQueue> KeyValueFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    if (helper()->storagePathType() == StoragePathType::FLAT)
        return readFlat(offset, size, m_blockSize);

    return readCanonical(offset, size);
}

folly::Future<std::size_t> KeyValueFileHandle::writeCanonical(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    return folly::via(m_executor.get(),
        [this, offset, locks = m_locks, writeCb = std::move(writeCb),
            helper = std::dynamic_pointer_cast<KeyValueAdapter>(this->helper())
                         ->helper(),
            buf = std::move(buf), self = shared_from_this()]() mutable {
            const auto size = buf.chainLength();
            if (size == 0 && offset > 0)
                return folly::makeFuture<std::size_t>(0);

            using one::logging::log_timer;
            using one::logging::csv::log;
            using one::logging::csv::read_write_perf;

            log_timer<> timer;
            if (!helper->hasRandomAccess() &&
                (size + offset > helper->maxCanonicalObjectSize())) {
                LOG(ERROR) << "Cannot write to object storage beyond "
                           << helper->maxCanonicalObjectSize();
                throw one::helpers::makePosixException(ERANGE);
            }

            Locks::accessor acc;
            locks->insert(acc, fileId());
            auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

            if (size > 0) {
                return folly::makeFuture<std::size_t>(static_cast<std::size_t>(
                    helper->modifyObject(fileId(), std::move(buf), offset)));
            }

            return folly::makeFuture<std::size_t>(
                static_cast<std::size_t>(size));
        });
}

folly::Future<std::size_t> KeyValueFileHandle::writeFlat(const off_t offset,
    folly::IOBufQueue buf, std::size_t storageBlockSize,
    WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    assert(storageBlockSize > 0);

    return folly::via(m_executor.get(),
        [this, offset, storageBlockSize, locks = m_locks,
            writeCb = std::move(writeCb),
            helper = std::dynamic_pointer_cast<KeyValueAdapter>(this->helper())
                         ->helper(),
            buf = std::move(buf), self = shared_from_this()]() mutable {
            const auto size = buf.chainLength();
            if (size == 0 && offset > 0)
                return folly::makeFuture<std::size_t>(0);

            using one::logging::log_timer;
            using one::logging::csv::log;
            using one::logging::csv::read_write_perf;

            log_timer<> timer;

            auto blockId = getBlockId(offset, storageBlockSize);
            auto blockOffset = getBlockOffset(offset, storageBlockSize);

            folly::fbvector<folly::Future<folly::Unit>> writeFutures;

            for (std::size_t bufOffset = 0; bufOffset < size;
                 blockOffset = 0, ++blockId) {

                const auto blockSize = std::min<std::size_t>(
                    storageBlockSize - blockOffset, size - bufOffset);

                auto writeFuture = via(m_executor.get(),
                    [this, iobuf = buf.front()->clone(), blockId, blockOffset,
                        bufOffset, blockSize,
                        self = shared_from_this()]() mutable {
                        folly::IOBufQueue bufq{
                            folly::IOBufQueue::cacheChainLength()};

                        bufq.append(std::move(iobuf));
                        bufq.trimStart(bufOffset);
                        bufq.trimEnd(bufq.chainLength() - blockSize);

                        return writeBlock(
                            std::move(bufq), blockId, blockOffset);
                    });

                writeFutures.emplace_back(std::move(writeFuture));

                bufOffset += blockSize;
            }

            return folly::collect(writeFutures)
                .via(m_executor.get())
                .thenValue([size, offset, fileId = fileId(),
                               writeCb = std::move(writeCb),
                               timer](std::vector<folly::Unit> && /*unused*/) {
                    log<read_write_perf>(fileId, "KeyValueFileHandle", "write",
                        offset, size, timer.stop());

                    if (writeCb)
                        writeCb(size);
                    return size;
                })
                .thenTry([](folly::Try<std::size_t> t) {
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

folly::Future<std::size_t> KeyValueFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    if (buf.empty())
        return folly::makeFuture<std::size_t>(0);

    if (helper()->storagePathType() == StoragePathType::FLAT)
        return writeFlat(
            offset, std::move(buf), m_blockSize, std::move(writeCb));

    return writeCanonical(offset, std::move(buf), std::move(writeCb));
}

const Timeout &KeyValueFileHandle::timeout() { return helper()->timeout(); }

KeyValueAdapter::KeyValueAdapter(std::shared_ptr<KeyValueHelper> helper,
    std::shared_ptr<KeyValueAdapterParams> params,
    std::shared_ptr<folly::Executor> executor,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_helper{std::move(helper)}
    , m_executor{std::move(executor)}
    , m_locks{std::make_shared<Locks>()}
{
    LOG_FCALL();

    invalidateParams()->setValue(std::move(params));
}

folly::fbstring KeyValueAdapter::name() const { return helper()->name(); }

folly::Future<folly::Unit> KeyValueAdapter::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId);

    if (helper()->storagePathType() == StoragePathType::FLAT &&
        currentSize == 0)
        return folly::makeFuture();

    // In case files on this storage are stored in single objects
    // simply remove the object
    if (helper()->storagePathType() == StoragePathType::CANONICAL) {
        return folly::via(m_executor.get(), [fileId, helper = m_helper] {
            helper->getObjectInfo(fileId);
            helper->deleteObject(fileId);
        });
    }

    // Calculate the list of objects into which the file has been
    // split on the storage
    folly::fbvector<folly::fbstring> keysToDelete;
    for (size_t objectId = 0; objectId <= (currentSize / blockSize());
         objectId++) {
        keysToDelete.emplace_back(m_helper->getKey(fileId, objectId));
    }

    // In case the storage supports batch delete (in a single request)
    // use it, otherwise schedule deletions of each object individually
    // in asynchronous requests
    if (m_helper->supportsBatchDelete()) {
        return folly::via(m_executor.get(),
            [keysToDelete = std::move(keysToDelete), helper = m_helper] {
                helper->deleteObjects(keysToDelete);
            });
    }

    auto futs = folly::window(
        keysToDelete,
        [helper = m_helper](const auto &keyToDelete) {
            helper->deleteObject(keyToDelete);
            return folly::makeFuture();
        },
        MAX_ASYNC_DELETE_OBJECTS);

    if (futs.empty())
        return folly::makeFuture();

    return folly::collectAll(futs.begin(), futs.end())
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<folly::Unit>> &&res) {
            std::for_each(res.begin(), res.end(),
                [](const auto &v) { v.throwIfFailed(); });
            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> KeyValueAdapter::mknod(const folly::fbstring &fileId,
    const mode_t mode, const FlagsSet &flags, const dev_t /*rdev*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    if (helper()->storagePathType() == StoragePathType::FLAT) {
        return folly::makeFuture();
    }

    if (!S_ISREG(flagsToMask(flags))) {
        LOG(ERROR)
            << "Ignoring mknod request for " << fileId << " with flags "
            << flagsToMask(flags)
            << " - only regular files can be created on object storages.";
        return folly::makeFuture();
    }

    return folly::via(
        m_executor.get(), [fileId, helper = m_helper, locks = m_locks] {
            // This try-catch is necessary in order to return EEXIST error
            // in case the object `fileId` already exists on the storage
            try {
                helper->getObjectInfo(fileId);
            }
            catch (const std::system_error &e) {
                if (e.code().value() == ENOENT) {
                    Locks::accessor acc;
                    locks->insert(acc, fileId);
                    auto g =
                        folly::makeGuard([&]() mutable { locks->erase(acc); });

                    helper->putObject(fileId,
                        folly::IOBufQueue{
                            folly::IOBufQueue::cacheChainLength()});

                    return folly::makeFuture();
                }
                throw e;
            }
            throw one::helpers::makePosixException(EEXIST);
        });
}

folly::Future<folly::Unit> KeyValueAdapter::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    if (static_cast<size_t>(size) == currentSize)
        return folly::makeFuture();

    // In case files on this storage are stored in single objects
    // simply remove the object
    if (helper()->storagePathType() == StoragePathType::CANONICAL) {
        return folly::via(m_executor.get(),
            [fileId, size, currentSize, helper = m_helper, locks = m_locks] {
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
            });
    }

    assert(blockSize() > 0);

    auto currentLastBlockId = getBlockId(currentSize, blockSize());
    auto newLastBlockId = getBlockId(size, blockSize());
    auto newLastBlockOffset = getBlockOffset(size, blockSize());

    // Generate a list of objects to delete
    folly::fbvector<folly::fbstring> keysToDelete;
    for (size_t objectId = 0; objectId <= currentLastBlockId; objectId++) {
        if ((objectId > newLastBlockId) ||
            ((objectId == newLastBlockId) && (newLastBlockOffset == 0))) {
            keysToDelete.emplace_back(m_helper->getKey(fileId, objectId));
        }
    }

    auto key = m_helper->getKey(fileId, newLastBlockId);
    auto remainderBlockSize = static_cast<std::size_t>(newLastBlockOffset);

    if (remainderBlockSize == 0 && newLastBlockId > 0) {
        key = m_helper->getKey(fileId, newLastBlockId - 1);
        remainderBlockSize = blockSize();
    }

    return folly::via(m_executor.get(),
        [fileId, remainderBlockSize, newLastBlockId, key, helper = m_helper,
            locks = m_locks] {
            if (remainderBlockSize > 0 || newLastBlockId > 0) {
                Locks::accessor acc;
                locks->insert(acc, key);
                auto g = folly::makeGuard([&]() mutable { locks->erase(acc); });

                try {
                    auto buf = fillToSize(
                        readBlock(helper, key, 0, remainderBlockSize),
                        remainderBlockSize);

                    // For storages which allow overwriting parts of objects
                    // (e.g. ceph), we have to remove the old object to ensure
                    // that truncated data was erased
                    if (helper->hasRandomAccess()) {
                        helper->deleteObject(key);
                    }

                    helper->putObject(key, std::move(buf));
                }
                catch (...) {
                    LOG(ERROR) << "Truncate failed due to unknown "
                                  "error during "
                                  "'fillToSize'";
                    throw;
                }
            }
        })
        .thenValue([keysToDelete = std::move(keysToDelete), helper = m_helper,
                       executor = m_executor](auto && /*unit*/) {
            if (keysToDelete.empty())
                return folly::makeFuture();

            if (helper->supportsBatchDelete()) {
                helper->deleteObjects(keysToDelete);
                return folly::makeFuture();
            }

            auto futs = folly::window(
                keysToDelete,
                [helper](const auto &keyToDelete) {
                    helper->deleteObject(keyToDelete);
                    return folly::makeFuture();
                },
                MAX_ASYNC_DELETE_OBJECTS);

            if (futs.empty())
                return folly::makeFuture();

            return folly::collectAll(futs.begin(), futs.end())
                .via(executor.get())
                .thenValue([](std::vector<folly::Try<folly::Unit>> &&res) {
                    std::for_each(res.begin(), res.end(),
                        [](const auto &v) { v.throwIfFailed(); });
                    return folly::makeFuture();
                });
        });
}

folly::Future<struct stat> KeyValueAdapter::getattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue(
            [fileId, helper = m_helper, locks = m_locks](
                auto && /*unit*/) { return helper->getObjectInfo(fileId); });
}

folly::Future<folly::Unit> KeyValueAdapter::access(
    const folly::fbstring &fileId, const int /*mask*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return getattr(fileId).thenValue(
        [](auto && /*unit*/) { return folly::makeFuture(); });
}

folly::Future<folly::Unit> KeyValueAdapter::multipartCopy(
    const folly::fbstring &sourceKey, const folly::fbstring &destinationKey,
    const std::size_t blockSize, const std::size_t size)
{
    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([sourceKey, destinationKey, blockSize, size,
                       helper = m_helper, locks = m_locks](auto && /*unit*/) {
            return helper->multipartCopy(
                sourceKey, destinationKey, blockSize, size);
        });
}

folly::Future<ListObjectsResult> KeyValueAdapter::listobjects(
    const folly::fbstring &prefix, const folly::fbstring &marker,
    const off_t offset, const size_t count)
{
    LOG_FCALL() << LOG_FARG(prefix) << LOG_FARG(marker) << LOG_FARG(offset)
                << LOG_FARG(count);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([prefix, marker, offset, count, helper = m_helper,
                       locks = m_locks](auto && /*unit*/) {
            return helper->listObjects(prefix, marker, offset, count);
        });
}

folly::Future<folly::IOBufQueue> KeyValueFileHandle::readBlocks(
    const off_t offset, const std::size_t requestedSize,
    const std::size_t storageBlockSize)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(requestedSize);

    const auto size = requestedSize;

    if (size == 0)
        return folly::makeFuture(
            folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()});

    folly::fbvector<folly::Future<folly::IOBufQueue>> readFutures;

    auto blockId = getBlockId(offset, storageBlockSize);
    auto blockOffset = getBlockOffset(offset, storageBlockSize);
    for (std::size_t bufOffset = 0; bufOffset < size;
         blockOffset = 0, ++blockId) {

        const auto blockSize =
            std::min(static_cast<std::size_t>(storageBlockSize - blockOffset),
                static_cast<std::size_t>(size - bufOffset));

        auto readFuture =
            folly::makeFuture()
                .via(m_executor.get())
                .thenValue([blockId, blockOffset, blockSize,
                               self = shared_from_this()](auto && /*unit*/) {
                    return fillToSize(
                        self->readBlock(blockId, blockOffset, blockSize),
                        blockSize);
                });

        readFutures.emplace_back(std::move(readFuture));
        bufOffset += blockSize;
    }

    return folly::collect(readFutures)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::IOBufQueue> &&results) {
            folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
            for (auto &subBuf : results)
                buf.append(std::move(subBuf));

            return buf;
        })
        .thenTry([](folly::Try<folly::IOBufQueue> &&t) {
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
        std::dynamic_pointer_cast<KeyValueAdapter>(this->helper())->helper();

    auto key = helper->getKey(fileId(), blockId);

    Locks::accessor acc;
    m_locks->insert(acc, key);
    auto g = folly::makeGuard([&]() mutable { m_locks->erase(acc); });

    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    log_timer<> timer;

    auto ret = ::readBlock(helper, key, blockOffset, size);

    log<read_write_perf>(key.toStdString(), "KeyValueFileHandle", "read",
        blockOffset, size, timer.stop());

    return ret;
}

void KeyValueFileHandle::writeBlock(
    folly::IOBufQueue buf, const uint64_t blockId, const off_t blockOffset)
{
    LOG_FCALL() << LOG_FARG(buf.chainLength()) << LOG_FARG(blockId)
                << LOG_FARG(blockOffset);

    auto helper =
        std::dynamic_pointer_cast<KeyValueAdapter>(this->helper())->helper();

    auto key = helper->getKey(fileId(), blockId);
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

            using one::logging::log_timer;
            using one::logging::csv::log;
            using one::logging::csv::read_write_perf;

            log_timer<> timer;

            auto filledSize = filledBuf.chainLength();
            helper->putObject(key, std::move(filledBuf));

            log<read_write_perf>(key.toStdString(), "KeyValueFileHandle",
                "write", blockOffset, filledSize, timer.stop());
        }
    }
    else {
        using one::logging::log_timer;
        using one::logging::csv::log;
        using one::logging::csv::read_write_perf;

        log_timer<> timer;

        auto size = buf.chainLength();
        helper->putObject(key, std::move(buf));

        log<read_write_perf>(key.toStdString(), "KeyValueFileHandle", "write",
            blockOffset, size);
    }
}

folly::Future<folly::Unit> KeyValueAdapter::fillMissingFileBlocks(
    const folly::fbstring &fileId, std::size_t size)
{
    size_t blockId = 0;
    std::vector<folly::Future<std::size_t>> futs;
    bool lastPart = false;

    while (blockId * blockSize() < size) {
        if ((blockId + 1) * blockSize() >= size)
            lastPart = true;
        auto key = helper()->getKey(fileId, blockId);
        auto fut =
            getattr(key)
                .thenValue([this, helper = helper(), lastPart, key](
                               auto &&attr) -> std::size_t {
                    if (!lastPart &&
                        (static_cast<std::size_t>(attr.st_size) <
                            blockSize())) {
                        return helper->putObject(key,
                            fillToSize(helper->getObject(key, 0, attr.st_size),
                                blockSize()));
                    }

                    return 0;
                })
                .thenError(folly::tag_t<std::system_error>{},
                    [this, key](auto && /*e*/) {
                        LOG_DBG(2) << "Creating empty null block: " << key;
                        return helper()->putObject(key,
                            fillToSize(
                                folly::IOBufQueue{
                                    folly::IOBufQueue::cacheChainLength()},
                                blockSize()));
                    });
        futs.emplace_back(std::move(fut));
        blockId++;
    }

    if (futs.empty())
        return folly::makeFuture();

    return folly::collectAll(futs.begin(), futs.end())
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<std::size_t>> &&res) {
            std::for_each(res.begin(), res.end(),
                [](const auto &v) { v.throwIfFailed(); });
            return folly::makeFuture();
        });
}

folly::Future<FileHandlePtr> KeyValueAdapter::open(
    const folly::fbstring &fileId, const int /*flags*/,
    const Params &openParams)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARGM(openParams);

    FileHandlePtr handle = std::make_shared<KeyValueFileHandle>(
        fileId, shared_from_this(), blockSize(), m_locks, m_executor);

    return folly::makeFuture(std::move(handle));
}

std::size_t KeyValueAdapter::blockSize() const noexcept
{
    LOG_FCALL();

    assert(m_helper.get() != nullptr);

    return m_helper->blockSize();
}

std::vector<folly::fbstring> KeyValueAdapter::handleOverridableParams() const
{
    return m_helper->getHandleOverridableParams();
}

} // namespace helpers
} // namespace one
