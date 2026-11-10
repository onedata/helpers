/**
 * @file nullDeviceHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_NULL_DEVICE_HELPER_H
#define HELPERS_NULL_DEVICE_HELPER_H

#include "helpers/storageHelper.h"

#include "flatOpScheduler.h"
#include "monitoring/monitoring.h"
#include "nullDeviceHelperParams.h"

#include <boost/thread/once.hpp>
#include <boost/variant.hpp>
#include <folly/executors/IOExecutor.h>
#if FUSE_USE_VERSION > 30
#include <fuse3/fuse.h>
#else
#include <fuse/fuse.h>
#endif

#include <chrono>
#include <random>

#undef signal_set

namespace one {
namespace helpers {

constexpr auto NULL_DEVICE_HELPER_CHAR = 'x';

class NullDeviceHelper;
class NullDeviceFileHandle;

using NullDeviceHelperPtr = std::shared_ptr<NullDeviceHelper>;
using NullDeviceFileHandlePtr = std::shared_ptr<NullDeviceFileHandle>;

/**
 * The @c FileHandle implementation for NullDevice storage helper.
 */
class NullDeviceFileHandle
    : public FileHandle,
      public std::enable_shared_from_this<NullDeviceFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId Path to the file under the root path.
     * @param helper Shared ptr to underlying helper.
     * @param executor Executor for driving async file operations.
     */
    static std::shared_ptr<NullDeviceFileHandle> create(
        const folly::fbstring &fileId, std::shared_ptr<NullDeviceHelper> helper,
        std::shared_ptr<folly::Executor> executor,
        Timeout timeout = constants::ASYNC_OPS_TIMEOUT);

    NullDeviceFileHandle(const NullDeviceFileHandle &) = delete;
    NullDeviceFileHandle &operator=(const NullDeviceFileHandle &) = delete;
    NullDeviceFileHandle(NullDeviceFileHandle &&) = delete;
    NullDeviceFileHandle &operator=(NullDeviceFileHandle &&) = delete;

    /**
     * Destructor.
     * Synchronously releases the file if @c sh_release or @c ash_release have
     * not been yet called.
     */
    ~NullDeviceFileHandle();

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    folly::Future<folly::Unit> release() override;

    folly::Future<folly::Unit> flush() override;

    folly::Future<folly::Unit> fsync(bool isDataSync) override;

    const Timeout &timeout() override { return m_timeout; }

    std::size_t readBytes() const { return m_readBytes.load(); }

    std::size_t writtenBytes() const { return m_writtenBytes.load(); }

    bool enableDataVerification() const { return m_enableDataVerification; }

    bool isConcurrencyEnabled() const override { return true; }

private:
    NullDeviceFileHandle(const folly::fbstring &fileId,
        std::shared_ptr<NullDeviceHelper> helper,
        std::shared_ptr<folly::Executor> executor,
        Timeout timeout = constants::ASYNC_OPS_TIMEOUT);

    template <typename T, typename F>
    folly::Future<T> simulateStorageIssues(
        folly::fbstring operationName, F &&func);

    void initOpScheduler();

    struct ReadOp {
        folly::Promise<folly::IOBufQueue> promise;
        off_t offset;
        std::size_t size;
        bool enableDataVerification;
        std::shared_ptr<cppmetrics::core::TimerContextBase> timer;
    };
    struct WriteOp {
        folly::Promise<std::size_t> promise;
        off_t offset;
        folly::IOBufQueue buf;
        std::shared_ptr<cppmetrics::core::TimerContextBase> timer;
    };
    struct FsyncOp {
        folly::Promise<folly::Unit> promise;
    };
    struct FlushOp {
        folly::Promise<folly::Unit> promise;
    };
    struct ReleaseOp {
        folly::Promise<folly::Unit> promise;
    };
    using HandleOp =
        boost::variant<ReadOp, WriteOp, FsyncOp, FlushOp, ReleaseOp>;

    friend struct OpExec;
    struct OpExec : public boost::static_visitor<> {
        explicit OpExec(const std::shared_ptr<NullDeviceFileHandle> &handle);
        std::unique_ptr<folly::Unit> startDrain() const;
        void operator()(ReadOp &op) const;
        void operator()(WriteOp &op) const;
        void operator()(FsyncOp &op) const;
        void operator()(FlushOp &op) const;
        void operator()(ReleaseOp &op) const;
        bool m_validCtx = false;
        std::weak_ptr<NullDeviceFileHandle> m_handle;
    };

    std::shared_ptr<folly::Executor> m_executor;
    std::shared_ptr<FlatOpScheduler<HandleOp, OpExec>> opScheduler;
    Timeout m_timeout;

    // The total number of bytes read since the file was opened
    std::atomic<std::size_t> m_readBytes;
    // The total number of bytes written since the file was opened
    std::atomic<std::size_t> m_writtenBytes;

    const bool m_enableDataVerification;

    // Use a preallocated, prefilled buffer for reads to avoid the cost of
    // memset on each read call.
    static std::vector<uint8_t> m_nullReadBuffer;
    static boost::once_flag m_nullReadBufferInitialized;

    static std::vector<uint8_t> m_nullReadPatternBuffer;
    static boost::once_flag m_nullReadPatternBufferInitialized;
};

/**
 * The NullDeviceHelper class provides a dummy storage helper acting as a null
 * device, i.e. accepting any operations with success. The read operations
 * always return empty values (e.g. read operation in a given range will return
 * a requested number of bytes all set to NULL_DEVICE_HELPER_CHAR).
 */
class NullDeviceHelper : public StorageHelper,
                         public std::enable_shared_from_this<NullDeviceHelper> {
public:
    using params_type = NullDeviceHelperParams;

    /**
     * Constructor.
     * @param latencyMin Minimum latency for operations in ms
     * @param latencyMax Maximum latency for operations in ms
     * @param timeoutProbability Probability that an operation will timeout
     *                           (0.0, 1.0)
     * @param filter Defines whic operations should be affected by latency and
     *               timeout, comma separated, empty or '*' enable for all
     *               operations
     * @param simulatedFilesystemParameters Parameters for a simulated null
     *                                      helper filesystem
     * @param simulatedFilesystemGrowSpeed Simulated filesystem grow speed in
     *                                     files per second
     * @param simulatedFileSize Simulated file system reported file size in
     *                          bytes.
     * @param enableDataVerification Enable data verification based on
     * predefined data pattern.
     * @param executor Executor for driving async file operations.
     */
    NullDeviceHelper(std::shared_ptr<NullDeviceHelperParams> params,
        std::shared_ptr<folly::Executor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    NullDeviceHelper(const NullDeviceHelper &) = delete;
    NullDeviceHelper &operator=(const NullDeviceHelper &) = delete;
    NullDeviceHelper(NullDeviceHelper &&) = delete;
    NullDeviceHelper &operator=(NullDeviceHelper &&) = delete;

    virtual ~NullDeviceHelper() = default;

    folly::fbstring name() const override { return NULL_DEVICE_HELPER_NAME; };

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count) override;

    folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t unmaskedMode, const FlagsSet &flags,
        const dev_t rdev) override;

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> chown(const folly::fbstring &fileId,
        const uid_t uid, const gid_t gid) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId) override;

    std::shared_ptr<folly::Executor> executor() override { return m_executor; }

    bool applies(const folly::fbstring &operationName) const;

    bool randomTimeout();

    int randomLatency();

    folly::Future<folly::Unit> simulateTimeout(
        const std::string &operationName);

    folly::Future<folly::Unit> simulateLatency(
        const std::string &operationName);

    bool isSimulatedFilesystem() const;

    /**
     * Returns the total number of entries (directories and files) on
     * a given filesystem tree level.
     * @param level Tree level
     */
    size_t simulatedFilesystemLevelEntryCount(size_t level);

    /**
     * Returns the total number of files directories in the simulated
     * filesystem.
     */
    size_t simulatedFilesystemEntryCount();

    /**
     * Returns a distance of the file or directory in the tree.
     * This distance is unique for each entry, and is calculated by
     * linearizing the tree from top to bottom and from left to right.
     * For instance the following specification:
     *
     *   2-2:2-2:0-1
     *
     * will generate the following filesystem tree:
     *
     *          1 2 3 4
     *          | |
     *          | +
     *          | 1 2 3 4
     *          | | |
     *          | + +
     *          | 1 1
     *          +
     *          1 2 3 4
     *          | |
     *          + +
     *          1 1
     *
     * which should result in the following numbering:
     *
     *          1 2 3 4
     *          | |
     *          | +
     *          | 9  10 11 12
     *          | |  |
     *          | +  +
     *          | 15 16
     *          +
     *          5  6 7 8
     *          |  |
     *          +  +
     *          13 14
     */
    size_t simulatedFilesystemFileDist(const std::vector<std::string> &path);

    folly::Future<folly::Unit> checkStorageAvailability() override;

    bool storageIssuesEnabled() const noexcept;

    HELPER_PARAM_GETTER(latencyMin)
    HELPER_PARAM_GETTER(latencyMax)
    HELPER_PARAM_GETTER(timeoutProbability)
    HELPER_PARAM_GETTER(filter)
    HELPER_PARAM_GETTER(simulatedFilesystemParameters)
    HELPER_PARAM_GETTER(simulatedFilesystemGrowSpeed)
    HELPER_PARAM_GETTER(simulatedFileSize)
    HELPER_PARAM_GETTER(enableDataVerification)
    HELPER_PARAM_GETTER(applyToAllOperations)

    template <typename T, typename F>
    folly::Future<T> simulateStorageIssues(
        const folly::fbstring &operationName, F &&func);

private:
    folly::Future<struct stat> getattrImpl(const folly::fbstring &fileId);

    folly::Future<folly::Unit> accessImpl(
        const folly::fbstring &fileId, const int mask);

    folly::Future<folly::fbvector<folly::fbstring>> readdirImpl(
        const folly::fbstring &fileId, off_t offset, size_t count);

    folly::Future<folly::fbstring> readlinkImpl(const folly::fbstring &fileId);

    folly::Future<folly::Unit> mknodImpl(const folly::fbstring &fileId,
        const mode_t unmaskedMode, const FlagsSet &flags, const dev_t rdev);

    folly::Future<folly::Unit> mkdirImpl(
        const folly::fbstring &fileId, const mode_t mode);

    folly::Future<folly::Unit> unlinkImpl(
        const folly::fbstring &fileId, const size_t currentSize);

    folly::Future<folly::Unit> rmdirImpl(const folly::fbstring &fileId);

    folly::Future<folly::Unit> symlinkImpl(
        const folly::fbstring &from, const folly::fbstring &to);

    folly::Future<folly::Unit> renameImpl(
        const folly::fbstring &from, const folly::fbstring &to);

    folly::Future<folly::Unit> linkImpl(
        const folly::fbstring &from, const folly::fbstring &to);

    folly::Future<folly::Unit> chmodImpl(
        const folly::fbstring &fileId, const mode_t mode);

    folly::Future<folly::Unit> chownImpl(
        const folly::fbstring &fileId, const uid_t uid, const gid_t gid);

    folly::Future<folly::Unit> truncateImpl(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize);

    folly::Future<FileHandlePtr> openImpl(const folly::fbstring &fileId,
        const int flags, const Params &openParams);

    folly::Future<folly::fbstring> getxattrImpl(
        const folly::fbstring &fileId, const folly::fbstring &name);

    folly::Future<folly::Unit> setxattrImpl(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace);

    folly::Future<folly::Unit> removexattrImpl(
        const folly::fbstring &fileId, const folly::fbstring &name);

    folly::Future<folly::fbvector<folly::fbstring>> listxattrImpl(
        const folly::fbstring &fileId);

    std::mt19937 m_randomGenerator(std::random_device());
    std::function<int()> m_latencyGenerator;
    std::function<double()> m_timeoutGenerator;
    bool m_simulatedFilesystemLevelEntryCountReady;
    std::vector<size_t> m_simulatedFilesystemLevelEntryCount;
    bool m_simulatedFilesystemEntryCountReady;
    size_t m_simulatedFilesystemEntryCount{};
    static std::chrono::time_point<std::chrono::system_clock> m_mountTime;

    std::shared_ptr<folly::Executor> m_executor;

    static boost::once_flag m_nullMountTimeOnceFlag;
};

/**
 * An implementation of @c StorageHelperFactory for null device storage helper.
 */
class NullDeviceHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param executor executor that will be used for some async operations.
     */
    explicit NullDeviceHelperFactory(
        std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
    }

    folly::fbstring name() const override { return NULL_DEVICE_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"latencyMin", "latencyMax", "timeoutProbability", "filter",
            "timeout", "enableDataVerification"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        return std::make_shared<NullDeviceHelper>(
            NullDeviceHelperParams::create(parameters), m_executor,
            executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_NULL_DEVICE_HELPER_H
