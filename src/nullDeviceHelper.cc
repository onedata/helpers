/**
 * @file nullDeviceHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nullDeviceHelper.h"

#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/filesystem.hpp>
#include <cerrno>
#include <folly/io/IOBuf.h>
#if FUSE_USE_VERSION > 30
#include <fuse3/fuse.h>
#elif
#include <fuse/fuse.h>
#endif

#include <cstring>
#include <map>
#include <string>
#include <utility>

namespace one {
namespace helpers {
constexpr auto NULL_DEVICE_HELPER_READ_PREALLOC_SIZE = 150 * 1024 * 1024;
constexpr auto NULL_DEVICE_HELPER_READLINK_SIZE = 10;
constexpr auto NULL_DEVICE_HELPER_READXATTR_SIZE = 10;

boost::once_flag NullDeviceFileHandle::m_nullReadBufferInitialized =
    BOOST_ONCE_INIT;
boost::once_flag NullDeviceHelper::m_nullMountTimeOnceFlag = BOOST_ONCE_INIT;
std::vector<uint8_t> NullDeviceFileHandle::m_nullReadBuffer = {};
std::chrono::time_point<std::chrono::system_clock>
    NullDeviceHelper::m_mountTime = {}; // NOLINT(cert-err58-cpp)

std::shared_ptr<NullDeviceFileHandle> NullDeviceFileHandle::create(
    const folly::fbstring &fileId, std::shared_ptr<NullDeviceHelper> helper,
    std::shared_ptr<folly::Executor> executor, Timeout timeout)
{
    auto ptr = std::shared_ptr<NullDeviceFileHandle>(new NullDeviceFileHandle(
        std::move(fileId), std::move(helper), std::move(executor), timeout));
    ptr->initOpScheduler();
    return ptr;
}

NullDeviceFileHandle::NullDeviceFileHandle(const folly::fbstring &fileId,
    std::shared_ptr<NullDeviceHelper> helper,
    std::shared_ptr<folly::Executor> executor, Timeout timeout)
    : FileHandle{fileId, std::move(helper)}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
    , m_readBytes{0}
    , m_writtenBytes{0}
{
    LOG_FCALL() << LOG_FARG(fileId);

    // Initilize the read buffer
    boost::call_once(
        [] {
            m_nullReadBuffer.reserve(NULL_DEVICE_HELPER_READ_PREALLOC_SIZE);
            std::fill_n(m_nullReadBuffer.begin(),
                NULL_DEVICE_HELPER_READ_PREALLOC_SIZE, NULL_DEVICE_HELPER_CHAR);
        },
        m_nullReadBufferInitialized);
}

NullDeviceFileHandle::~NullDeviceFileHandle() { LOG_FCALL(); }

template <typename T, typename F>
folly::Future<T> NullDeviceFileHandle::simulateStorageIssues(
    folly::fbstring operationName, F &&func)
{
    return std::dynamic_pointer_cast<NullDeviceHelper>(helper())
        ->simulateStorageIssues<T, F>(
            std::move(operationName), std::forward<F>(func));
}

NullDeviceFileHandle::OpExec::OpExec(
    const std::shared_ptr<NullDeviceFileHandle> &handle)
    : m_handle{handle}
{
}

std::unique_ptr<folly::Unit> NullDeviceFileHandle::OpExec::startDrain() const
{
    if (auto handle = m_handle.lock()) {
        return std::make_unique<folly::Unit>();
    }

    return std::unique_ptr<folly::Unit>{nullptr};
}

void NullDeviceFileHandle::initOpScheduler()
{
    auto self = shared_from_this();
    opScheduler = FlatOpScheduler<HandleOp, OpExec>::create(
        m_executor, std::make_shared<OpExec>(self));
}

folly::Future<folly::IOBufQueue> NullDeviceFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.nulldevice.read");

    return simulateStorageIssues<folly::Unit>("read", []() {})
        .thenValue([this, offset, size, timer = std::move(timer)](
                       auto && /*unit*/) mutable {
            return opScheduler->schedule(
                ReadOp{{}, offset, size, std::move(timer)});
        });
}

void NullDeviceFileHandle::OpExec::operator()(ReadOp &op) const
{
    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    const auto &size = op.size;
    const auto &offset = op.offset;
    const auto &fileId = handle->fileId();
    auto &timer = op.timer;

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};

    LOG_DBG(2) << "Attempting to read " << size << " bytes at offset " << offset
               << " from file " << fileId;

    if (size < NULL_DEVICE_HELPER_READ_PREALLOC_SIZE) {
        auto nullBuf = folly::IOBuf::wrapBuffer(m_nullReadBuffer.data(), size);
        buf.append(std::move(nullBuf));
    }
    else {
        void *data = buf.preallocate(size, size).first;
        memset(data, NULL_DEVICE_HELPER_CHAR, size);
        buf.postallocate(size);
    }

    LOG_DBG(2) << "Read " << size << " bytes from file " << fileId;

    handle->m_readBytes += size;

    ONE_METRIC_TIMERCTX_STOP(timer, size);

    op.promise.setValue(std::move(buf));
}

folly::Future<std::size_t> NullDeviceFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    auto timer =
        ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.nulldevice.write");

    return opScheduler->schedule(WriteOp{{}, offset, std::move(buf)})
        .thenValue([this](std::size_t &&written) {
            return simulateStorageIssues<std::size_t>(
                "write", [written] { return written; });
        })
        .thenValue([writeCb = std::move(writeCb), timer = std::move(timer)](
                       std::size_t &&written) {
            if (writeCb)
                writeCb(written);
            return written;
        });
}

void NullDeviceFileHandle::OpExec::operator()(WriteOp &op) const
{
    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    auto &buf = op.buf;
    const auto &fileId = handle->fileId();
    auto &timer = op.timer;

    std::size_t size = buf.chainLength();

    LOG_DBG(2) << "Written " << size << " bytes to file " << fileId;

    handle->m_writtenBytes += size;

    ONE_METRIC_TIMERCTX_STOP(timer, size);

    op.promise.setValue(size);
}

void NullDeviceFileHandle::OpExec::operator()(ReleaseOp &op) const
{
    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    auto &fileId = handle->fileId();

    ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.release");

    LOG_DBG(2) << "Closing file " << fileId;

    op.promise.setValue();
}

folly::Future<folly::Unit> NullDeviceFileHandle::release()
{
    LOG_FCALL();
    return opScheduler->schedule(ReleaseOp{})
        .thenValue([self = shared_from_this()](auto && /*unit*/) {
            return self->simulateStorageIssues<folly::Unit>("release", [] {});
        });
}

void NullDeviceFileHandle::OpExec::operator()(FlushOp &op) const
{
    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    auto &fileId = handle->fileId();

    LOG_DBG(2) << "Flushing file " << fileId;

    op.promise.setValue();
}

folly::Future<folly::Unit> NullDeviceFileHandle::flush()
{
    LOG_FCALL();
    return opScheduler->schedule(FlushOp{}).thenValue(
        [self = shared_from_this()](auto && /*unit*/) {
            return self->simulateStorageIssues<folly::Unit>("flush", [] {});
        });
}

void NullDeviceFileHandle::OpExec::operator()(FsyncOp &op) const
{
    auto handle = m_handle.lock();
    if (!handle) {
        op.promise.setException(makePosixException(ECANCELED));
        return;
    }

    auto &fileId = handle->fileId();

    ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.fsync");

    LOG_DBG(2) << "Syncing file " << fileId;

    op.promise.setValue();
}

folly::Future<folly::Unit> NullDeviceFileHandle::fsync(bool /*isDataSync*/)
{
    LOG_FCALL();
    return opScheduler->schedule(FsyncOp{}).thenValue(
        [self = shared_from_this()](auto && /*unit*/) {
            return self->simulateStorageIssues<folly::Unit>("fsync", [] {});
        });
}

NullDeviceHelper::NullDeviceHelper(const int latencyMin, const int latencyMax,
    const double timeoutProbability, const folly::fbstring &filter,
    std::vector<std::pair<int64_t, int64_t>> simulatedFilesystemParameters,
    double simulatedFilesystemGrowSpeed, size_t simulatedFileSize,
    std::shared_ptr<folly::Executor> executor, Timeout timeout,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_latencyMin{latencyMin}
    , m_latencyMax{latencyMax}
    , m_latencyGenerator{std::bind(
          std::uniform_int_distribution<>(latencyMin, latencyMax),
          std::default_random_engine(std::random_device{}()))}
    , m_timeoutGenerator{std::bind(std::uniform_real_distribution<>(0.0, 1.0),
          std::default_random_engine(std::random_device{}()))}
    , m_timeoutProbability{timeoutProbability}
    , m_simulatedFilesystemParameters{std::move(simulatedFilesystemParameters)}
    , m_simulatedFilesystemGrowSpeed{simulatedFilesystemGrowSpeed}
    , m_simulatedFileSize{simulatedFileSize}
    , m_simulatedFilesystemLevelEntryCountReady{false}
    , m_simulatedFilesystemEntryCountReady{false}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
{
    LOG_FCALL() << LOG_FARG(latencyMin) << LOG_FARG(latencyMax)
                << LOG_FARG(timeoutProbability) << LOG_FARG(filter);

    if (filter.empty() || filter == "*") {
        m_applyToAllOperations = true;
    }
    else {
        m_applyToAllOperations = false;
        folly::split(",", filter, m_filter, true);
        std::for_each(m_filter.begin(), m_filter.end(), [](auto &token) {
            boost::algorithm::trim(token);
            boost::algorithm::to_lower(token);
        });
    }

    // Initialize the mount time safely to the creation time of first
    // helper instance
    boost::call_once(
        []() {
            NullDeviceHelper::m_mountTime = std::chrono::system_clock::now();
        },
        m_nullMountTimeOnceFlag);

    // Precalculate the number of entries per level and total in the
    // filesystem
    simulatedFilesystemEntryCount();
}

bool NullDeviceHelper::storageIssuesEnabled() const noexcept
{
    return (m_latencyMax > 0.0) || (m_timeoutProbability > 0.0);
}

template <typename T, typename F>
folly::Future<T> NullDeviceHelper::simulateStorageIssues(
    const folly::fbstring &operationName, F &&func)
{
    if (storageIssuesEnabled())
        return folly::makeFuture()
            .via(m_executor.get())
            .thenValue([this, operationName](auto && /*unit*/) {
                return simulateLatency(operationName.toStdString());
            })
            .thenValue([this, operationName](auto && /*unit*/) {
                return simulateTimeout(operationName.toStdString());
            })
            .thenValue([func = std::forward<F>(func)](
                           auto && /*unit*/) { return func(); });

    return folly::via(
        m_executor.get(), [func = std::forward<F>(func)] { return func(); });
}

folly::Future<struct stat> NullDeviceHelper::getattr(
    const folly::fbstring &fileId)
{
    return simulateStorageIssues<struct stat>(
        "getattr", [fileId, self = shared_from_this()] {
            return self->getattrImpl(fileId);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    return simulateStorageIssues<folly::Unit>(
        "access", [fileId, mask, self = shared_from_this()] {
            return self->accessImpl(fileId, mask);
        });
}

folly::Future<folly::fbvector<folly::fbstring>> NullDeviceHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    return simulateStorageIssues<folly::fbvector<folly::fbstring>>(
        "readdir", [fileId, offset, count, self = shared_from_this()] {
            return self->readdirImpl(fileId, offset, count);
        });
}

folly::Future<folly::fbstring> NullDeviceHelper::readlink(
    const folly::fbstring &fileId)
{
    return simulateStorageIssues<folly::fbstring>(
        "readlink", [fileId, self = shared_from_this()] {
            return self->readlinkImpl(fileId);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::mknod(
    const folly::fbstring &fileId, const mode_t unmaskedMode,
    const FlagsSet &flags, const dev_t rdev)
{
    return simulateStorageIssues<folly::Unit>("mknod",
        [fileId, unmaskedMode, flags, rdev, self = shared_from_this()] {
            return self->mknodImpl(fileId, unmaskedMode, flags, rdev);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::mkdir(
    const folly::fbstring &fileId, const mode_t mode)
{
    return simulateStorageIssues<folly::Unit>(
        "mkdir", [fileId, mode, self = shared_from_this()] {
            return self->mkdirImpl(fileId, mode);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::unlink(
    const folly::fbstring &fileId, const size_t currentSize)
{
    return simulateStorageIssues<folly::Unit>(
        "unlink", [fileId, currentSize, self = shared_from_this()] {
            return self->unlinkImpl(fileId, currentSize);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::rmdir(
    const folly::fbstring &fileId)
{
    return simulateStorageIssues<folly::Unit>(
        "rmdir", [fileId, self = shared_from_this()] {
            return self->rmdirImpl(fileId);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::symlink(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return simulateStorageIssues<folly::Unit>(
        "symlink", [from, to, self = shared_from_this()] {
            return self->symlinkImpl(from, to);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return simulateStorageIssues<folly::Unit>(
        "rename", [from, to, self = shared_from_this()] {
            return self->renameImpl(from, to);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::link(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return simulateStorageIssues<folly::Unit>(
        "link", [from, to, self = shared_from_this()] {
            return self->linkImpl(from, to);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::chmod(
    const folly::fbstring &fileId, const mode_t mode)
{
    return simulateStorageIssues<folly::Unit>(
        "chmod", [fileId, mode, self = shared_from_this()] {
            return self->chmodImpl(fileId, mode);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::chown(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    return simulateStorageIssues<folly::Unit>(
        "chown", [fileId, uid, gid, self = shared_from_this()] {
            return self->chownImpl(fileId, uid, gid);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::truncate(
    const folly::fbstring &fileId, const off_t size, const size_t currentSize)
{
    return simulateStorageIssues<folly::Unit>(
        "truncate", [fileId, size, currentSize, self = shared_from_this()] {
            return self->truncateImpl(fileId, size, currentSize);
        });
}

folly::Future<FileHandlePtr> NullDeviceHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    return simulateStorageIssues<FileHandlePtr>(
        "open", [fileId, flags, openParams, self = shared_from_this()] {
            return self->openImpl(fileId, flags, openParams);
        });
}

folly::Future<folly::fbstring> NullDeviceHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    return simulateStorageIssues<folly::fbstring>(
        "getxattr", [fileId, name, self = shared_from_this()] {
            return self->getxattrImpl(fileId, name);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::setxattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const folly::fbstring &value, bool create, bool replace)
{
    return simulateStorageIssues<folly::Unit>("setxattr",
        [fileId, name, value, create, replace, self = shared_from_this()] {
            return self->setxattrImpl(fileId, name, value, create, replace);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    return simulateStorageIssues<folly::Unit>(
        "removexattr", [fileId, name, self = shared_from_this()] {
            return self->removexattrImpl(fileId, name);
        });
}

folly::Future<folly::fbvector<folly::fbstring>> NullDeviceHelper::listxattr(
    const folly::fbstring &fileId)
{
    return simulateStorageIssues<folly::fbvector<folly::fbstring>>(
        "listxattr", [fileId, self = shared_from_this()] {
            return self->listxattrImpl(fileId);
        });
}

folly::Future<struct stat> NullDeviceHelper::getattrImpl(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([this, fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.getattr");

            LOG_DBG(2) << "Attempting to stat file " << fileId;

            constexpr auto ST_MODE_MASK = 0777;

            struct stat stbuf = {};
            stbuf.st_gid = 0;
            stbuf.st_uid = 0;

            if (isSimulatedFilesystem()) {
#if !defined(__APPLE__)
                std::vector<std::string> pathTokens;
                auto fileIdStr = fileId.toStdString();
                folly::split("/", fileIdStr, pathTokens, true);
                auto level = pathTokens.size();

                if (level == 0) {
                    stbuf.st_mode = ST_MODE_MASK | S_IFDIR;
                }
                else {
                    auto pathLeaf = std::stol(pathTokens[level - 1]);

                    if (pathLeaf <
                        std::get<0>(
                            m_simulatedFilesystemParameters[level - 1])) {
                        stbuf.st_mode = ST_MODE_MASK | S_IFDIR;
                    }
                    else {
                        stbuf.st_mode = ST_MODE_MASK | S_IFREG;
                        stbuf.st_size = m_simulatedFileSize;
                    }
                }

                stbuf.st_ino = simulatedFilesystemFileDist(pathTokens) + 2;

                stbuf.st_ctim.tv_sec =
                    std::chrono::system_clock::to_time_t(m_mountTime);
                stbuf.st_ctim.tv_nsec = 0;

                if (m_simulatedFilesystemGrowSpeed == 0.0) {
                    stbuf.st_mtim.tv_sec =
                        std::chrono::system_clock::to_time_t(m_mountTime);
                    stbuf.st_mtim.tv_nsec = 0;
                }
                else {
                    if ((stbuf.st_mode & S_IFDIR) != 0u) {
                        auto now = std::chrono::system_clock::now();
                        auto mtimeMax = m_mountTime +
                            std::chrono::seconds(static_cast<int64_t>(
                                m_simulatedFilesystemEntryCount /
                                m_simulatedFilesystemGrowSpeed));

                        if (now < mtimeMax)
                            stbuf.st_mtim.tv_sec =
                                std::chrono::system_clock::to_time_t(now);
                        else
                            stbuf.st_mtim.tv_sec =
                                std::chrono::system_clock::to_time_t(mtimeMax);
                    }
                    else {
                        stbuf.st_mtim.tv_sec =
                            std::chrono::system_clock::to_time_t(m_mountTime +
                                std::chrono::seconds(
                                    static_cast<int64_t>(stbuf.st_ino /
                                        m_simulatedFilesystemGrowSpeed)));
                    }
                    stbuf.st_mtim.tv_nsec = 0;
                }
#endif
            }
            else {
                stbuf.st_ino = 1;
                stbuf.st_mode = ST_MODE_MASK | S_IFREG;
                stbuf.st_size = 0;
            }

            return folly::makeFuture(stbuf);
        });
}

folly::Future<folly::Unit> NullDeviceHelper::accessImpl(
    const folly::fbstring &fileId, const int mask)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mask);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.access");

            LOG_DBG(2) << "Attempting to access file " << fileId;

            return folly::makeFuture();
        });
}

folly::Future<folly::fbvector<folly::fbstring>> NullDeviceHelper::readdirImpl(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([this, fileId, offset, count, self = shared_from_this()](
                       auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.readdir");

            folly::fbvector<folly::fbstring> ret;

            LOG_DBG(2) << "Attempting to read directory " << fileId;

            if (isSimulatedFilesystem()) {
                std::vector<std::string> pathTokens;
                auto fileIdStr = fileId.toStdString();
                folly::split("/", fileIdStr, pathTokens, true);

                auto level = pathTokens.size();

                if (level < m_simulatedFilesystemParameters.size()) {
                    size_t totalEntriesOnLevel =
                        std::get<0>(m_simulatedFilesystemParameters[level]) +
                        std::get<1>(m_simulatedFilesystemParameters[level]);

                    for (size_t i = offset; i <
                         std::min<size_t>(offset + count, totalEntriesOnLevel);
                         i++) {
                        auto entryId = std::to_string(i);
                        auto entryPath =
                            boost::filesystem::path(fileId.toStdString()) /
                            boost::filesystem::path(entryId);

                        std::vector<std::string> entryPathTokens;
                        folly::split(
                            "/", entryPath.string(), entryPathTokens, true);

                        if (m_simulatedFilesystemGrowSpeed == 0.0 ||
                            (std::chrono::seconds(
                                 simulatedFilesystemFileDist(entryPathTokens)) /
                                m_simulatedFilesystemGrowSpeed) +
                                    m_mountTime <
                                std::chrono::system_clock::now()) {
                            ret.emplace_back(entryId);
                        }
                    }
                }
            }
            else {
                // we want the files to have unique names even when listing
                // thousands of files
                for (size_t i = 0; i < count; i++)
                    ret.emplace_back(std::to_string(i + offset));
            }

            LOG_DBG(2) << "Read directory " << fileId << " at offset " << offset
                       << " with entries " << LOG_VEC(ret);

            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(ret));
        });
}

folly::Future<folly::fbstring> NullDeviceHelper::readlinkImpl(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.readlink");

            LOG_DBG(2) << "Attempting to read link " << fileId;

            auto target = folly::fbstring(
                NULL_DEVICE_HELPER_READLINK_SIZE, NULL_DEVICE_HELPER_CHAR);

            LOG_DBG(2) << "Read link " << fileId << " - resolves to " << target;

            return folly::makeFuture(std::move(target));
        });
}

folly::Future<folly::Unit> NullDeviceHelper::mknodImpl(
    const folly::fbstring &fileId, const mode_t unmaskedMode,
    const FlagsSet &flags, const dev_t /*rdev*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(unmaskedMode)
                << LOG_FARG(flagsToMask(flags));

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.mknod");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::mkdirImpl(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.mkdir");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::unlinkImpl(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.unlink");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::rmdirImpl(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.rmdir");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::symlinkImpl(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([from = from, to = to, self = shared_from_this()](
                       auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.symlink");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::renameImpl(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([from = from, to = to, self = shared_from_this()](
                       auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.rename");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::linkImpl(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([from = from, to = to, self = shared_from_this()](
                       auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.link");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::chmodImpl(
    const folly::fbstring &fileId, const mode_t mode)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(mode);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.chmod");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::chownImpl(
    const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(uid) << LOG_FARG(gid);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.chown");

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::truncateImpl(
    const folly::fbstring &fileId, const off_t size,
    const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.truncate");

            return folly::makeFuture();
        });
}

folly::Future<FileHandlePtr> NullDeviceHelper::openImpl(
    const folly::fbstring &fileId, const int flags,
    const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(flags);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, executor = m_executor, timeout = m_timeout,
                       self = shared_from_this()](auto && /*unit*/) mutable {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.open");

            auto handle = NullDeviceFileHandle::create(
                fileId, self, std::move(executor), timeout);

            return folly::makeFuture<FileHandlePtr>(std::move(handle));
        });
}

folly::Future<folly::fbstring> NullDeviceHelper::getxattrImpl(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, name, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.getxattr");

            return folly::makeFuture(folly::fbstring(
                NULL_DEVICE_HELPER_READXATTR_SIZE, NULL_DEVICE_HELPER_CHAR));
        });
}

folly::Future<folly::Unit> NullDeviceHelper::setxattrImpl(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const folly::fbstring &value, bool create, bool replace)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name) << LOG_FARG(value)
                << LOG_FARG(create) << LOG_FARG(replace);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, name, value, create, replace,
                       self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.setxattr");

            if (create && replace) {
                return makeFuturePosixException<folly::Unit>(EINVAL);
            }

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::removexattrImpl(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, name, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.removexattr");

            return folly::makeFuture();
        });
}

folly::Future<folly::fbvector<folly::fbstring>> NullDeviceHelper::listxattrImpl(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([fileId, self = shared_from_this()](auto && /*unit*/) {
            ONE_METRIC_COUNTER_INC("comp.helpers.mod.nulldevice.listxattr");

            folly::fbvector<folly::fbstring> ret;

            for (int i = 1; i <= NULL_DEVICE_HELPER_READXATTR_SIZE; i++) {
                ret.emplace_back(std::string(i, NULL_DEVICE_HELPER_CHAR));
            }

            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(ret));
        });
}

int NullDeviceHelper::randomLatency() { return m_latencyGenerator(); }

bool NullDeviceHelper::randomTimeout()
{
    return m_timeoutGenerator() < m_timeoutProbability;
}

bool NullDeviceHelper::applies(const folly::fbstring &operationName)
{
    return m_applyToAllOperations ||
        (std::find(m_filter.begin(), m_filter.end(),
             operationName.toStdString()) != m_filter.end());
}

folly::Future<folly::Unit> NullDeviceHelper::simulateTimeout(
    const std::string &operationName)
{
    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([this, operationName](auto && /*unit*/) {
            if (applies(operationName) && randomTimeout())
                throw one::helpers::makePosixException(EAGAIN);

            return folly::makeFuture();
        });
}

folly::Future<folly::Unit> NullDeviceHelper::simulateLatency(
    const std::string &operationName)
{
    return folly::makeFuture()
        .via(m_executor.get())
        .thenValue([this, operationName](auto && /*unit*/) {
            if (applies(operationName)) {
                return folly::makeFuture().delayed(
                    std::chrono::milliseconds(randomLatency()));
            }

            return folly::makeFuture();
        });
}

bool NullDeviceHelper::isSimulatedFilesystem() const
{
    return !m_simulatedFilesystemParameters.empty();
}

std::vector<std::pair<int64_t, int64_t>>
NullDeviceHelper::simulatedFilesystemParameters() const
{
    return m_simulatedFilesystemParameters;
}

double NullDeviceHelper::simulatedFilesystemGrowSpeed() const
{
    return m_simulatedFilesystemGrowSpeed;
}

size_t NullDeviceHelper::simulatedFileSize() const
{
    return m_simulatedFileSize;
}

size_t NullDeviceHelper::simulatedFilesystemLevelEntryCount(size_t level)
{
    if (!m_simulatedFilesystemLevelEntryCountReady) {
        for (size_t l = 0; l < m_simulatedFilesystemParameters.size(); l++) {
            size_t directoryProduct = 1;
            for (size_t i = 0; i < l; i++)
                directoryProduct *=
                    std::get<0>(m_simulatedFilesystemParameters[i]);

            m_simulatedFilesystemLevelEntryCount.push_back(directoryProduct *
                (std::get<0>(m_simulatedFilesystemParameters[l]) +
                    std::get<1>(m_simulatedFilesystemParameters[l])));
        }
    }

    if (level >= m_simulatedFilesystemParameters.size())
        level = m_simulatedFilesystemParameters.size() - 1;

    return m_simulatedFilesystemLevelEntryCount[level];
}

size_t NullDeviceHelper::simulatedFilesystemEntryCount()
{
    if (!m_simulatedFilesystemEntryCountReady) {
        m_simulatedFilesystemEntryCount = 0;

        for (size_t i = 0; i < m_simulatedFilesystemParameters.size(); i++) {
            m_simulatedFilesystemEntryCount +=
                simulatedFilesystemLevelEntryCount(i);
        }
    }

    return m_simulatedFilesystemEntryCount;
}

size_t NullDeviceHelper::simulatedFilesystemFileDist(
    const std::vector<std::string> &path)
{
    if (path.empty())
        return 0;

    size_t level = path.size() - 1;
    size_t distance = 0;
    for (size_t i = 0; i < level; i++) {
        distance += simulatedFilesystemLevelEntryCount(i);
    }

    std::vector<size_t> parentPath;
    for (size_t i = 0; i < path.size() - 1; i++) {
        parentPath.emplace_back(std::stol(path[i]));
    }

    size_t pathDirProduct = std::accumulate(
        parentPath.begin(), parentPath.end(), 1, std::multiplies<>());

    auto levelDirs = std::get<0>(m_simulatedFilesystemParameters[level]);
    auto levelFiles = std::get<1>(m_simulatedFilesystemParameters[level]);

    distance += (levelDirs + levelFiles) * (pathDirProduct - 1) +
        std::stol(path[path.size() - 1]) + 1;

    return distance - 1;
}

std::pair<std::vector<std::pair<int64_t, int64_t>>, folly::Optional<size_t>>
NullDeviceHelperFactory::parseSimulatedFilesystemParameters(
    const std::string &params)
{
    auto result = std::vector<std::pair<int64_t, int64_t>>{};

    folly::Optional<size_t> fileSize{};
    if (params.empty())
        return {result, fileSize};

    auto levels = std::vector<std::string>{};

    folly::split(":", params, levels, true);

    auto i = 0U;

    for (const auto &level : levels) {
        auto levelParams = std::vector<std::string>{};
        folly::split("-", level, levelParams, true);

        if (levelParams.size() != 2) {
            if ((i == levels.size() - 1) && (levelParams.size() == 1)) {
                fileSize.emplace(std::stoull(levelParams[0]));
            }
            else {
                throw std::invalid_argument("Invalid null helper simulated "
                                            "filesystem parameters "
                                            "specification: '" +
                    params + "'");
            }
        }
        else
            result.emplace_back(
                std::stol(levelParams[0]), std::stol(levelParams[1]));

        i++;
    }
    return {result, fileSize};
}
} // namespace helpers
} // namespace one
