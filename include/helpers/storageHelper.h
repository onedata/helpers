/**
 * @file storageHelper.h
 * @author Rafal Slota
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_STORAGE_HELPER_H
#define HELPERS_STORAGE_HELPER_H

#include "logging.h"

#include <fuse.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(__linux__)
#include <linux/limits.h>
#else
#define XATTR_SIZE_MAX (64 * 1024)
#endif

#include <asio/buffer.hpp>
#include <asio/io_service.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/lexical_cast.hpp>
#include <folly/FBString.h>
#include <folly/FBVector.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/io/IOBufQueue.h>
#include <tbb/concurrent_hash_map.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace one {
namespace helpers {

using ListObjectsResult =
    folly::fbvector<std::tuple<folly::fbstring, struct stat>>;

#if WITH_CEPH
constexpr auto CEPH_HELPER_NAME = "ceph";
constexpr auto CEPHRADOS_HELPER_NAME = "cephrados";
#endif

constexpr auto POSIX_HELPER_NAME = "posix";

constexpr auto PROXY_HELPER_NAME = "proxy";

constexpr auto NULL_DEVICE_HELPER_NAME = "nulldevice";

#if WITH_S3
constexpr auto S3_HELPER_NAME = "s3";
#endif

#if WITH_SWIFT
constexpr auto SWIFT_HELPER_NAME = "swift";
#endif

#if WITH_GLUSTERFS
constexpr auto GLUSTERFS_HELPER_NAME = "glusterfs";
#endif

#if WITH_WEBDAV
constexpr auto WEBDAV_HELPER_NAME = "webdav";
constexpr auto HTTP_HELPER_NAME = "http";
#endif

#if WITH_XROOTD
constexpr auto XROOTD_HELPER_NAME = "xrootd";
#endif

namespace {
constexpr std::chrono::milliseconds ASYNC_OPS_TIMEOUT{120000};
const std::error_code SUCCESS_CODE{};
constexpr int IO_RETRY_COUNT{4};
constexpr std::chrono::milliseconds IO_RETRY_INITIAL_DELAY{10};
constexpr float IO_RETRY_DELAY_BACKOFF_FACTOR{5.0};
} // namespace

/**
 * Generic retry function wrapper.
 * @param op Function to repeat.
 * @param condition Function to use to test the result returned by @c op,
 *                  when evaluates to true the result is returned.
 * @param retryCount Maximum number of retries.
 * @param retryInitialDelay Delay before first retry.
 * @param retryBackoff Factor determining the retry delay increase.
 * @return Last returned value of @c op.
 */
template <typename OpFunc, typename CondFunc>
inline auto retry(OpFunc &&op, CondFunc &&condition,
    int retryCount = IO_RETRY_COUNT,
    std::chrono::milliseconds retryInitialDelay = IO_RETRY_INITIAL_DELAY,
    float retryBackoff = IO_RETRY_DELAY_BACKOFF_FACTOR)
{
    auto ret = op();
    auto retryIt = 0;

    while (!condition(ret) && (retryIt < retryCount)) {
        std::this_thread::sleep_for(
            retryInitialDelay * std::pow(retryBackoff, retryIt));

        ret = op();
        retryIt++;
    }

    return ret;
}

/**
 * Creates an instance of @c std::error_code in @c std::system_category that
 * corresponds to a given POSIX error code.
 * @param posixCode The POSIX error code to translate.
 * @return @c std::error_code instance corresponding to the error code.
 */
inline std::error_code makePosixError(const int posixCode)
{
    return std::error_code{std::abs(posixCode), std::system_category()};
}

inline std::system_error makePosixException(const int posixCode)
{
    return std::system_error{one::helpers::makePosixError(posixCode)};
}

template <typename T = folly::Unit>
inline folly::Future<T> makeFuturePosixException(const int posixCode)
{
    return folly::makeFuture<T>(makePosixException(posixCode));
}

/**
 * Convert string octal representation of POSIX permissions, e.g. '0755'
 * to mode_t value.
 *
 * @param p POSIX param string
 */
mode_t parsePosixPermissions(folly::fbstring p);

/**
 * Open flags recognized by helpers.
 */
enum class Flag {
    NONBLOCK,
    APPEND,
    ASYNC,
    FSYNC,
    NOFOLLOW,
    CREAT,
    TRUNC,
    EXCL,
    RDONLY,
    WRONLY,
    RDWR,
    IFREG,
    IFCHR,
    IFBLK,
    IFIFO,
    IFSOCK
};

struct FlagHash {
    template <typename T> std::size_t operator()(T t) const
    {
        return static_cast<std::size_t>(t);
    }
};

/**
 * Determine whether helper is executed within Oneclient or Oneprovider process.
 */
enum class ExecutionContext { ONEPROVIDER, ONECLIENT };

/**
 * Determines between different types of paths under which files are stored on
 * actual storage with respect to logical paths in virtual file system
 */
enum class StoragePathType {
    CANONICAL, // same as logical paths in virtual file system, relative to
               // space
    FLAT       // custom path scheme based on unique file id
};

class FileHandle;
class StorageHelper;
class StorageHelperParams;
using FlagsSet = std::unordered_set<Flag, FlagHash>;
using Params = std::unordered_map<folly::fbstring, folly::fbstring>;
using StorageHelperPtr = std::shared_ptr<StorageHelper>;
using FileHandlePtr = std::shared_ptr<FileHandle>;
using Timeout = std::chrono::milliseconds;
using WriteCallback = std::function<void(std::size_t)>;

template <class... T>
using GeneralCallback = std::function<void(T..., std::error_code)>;
using VoidCallback = GeneralCallback<>;

/**
 * @param flags A set of @c Flag values to translate.
 * @returns A POSIX-compatible bitmask representing given flags.
 */
int flagsToMask(const FlagsSet &flags);

/**
 * @param mask A POSIX-compatible bitmask.
 * @returns A set of @c Flag values representing given flags.
 */
FlagsSet maskToFlags(int mask);

/**
 * An exception reporting a missing parameter for helper creation.
 */
class MissingParameterException : public std::out_of_range {
public:
    /**
     * Constructor.
     * @param whatArg Name of the missing parameter.
     */
    MissingParameterException(const folly::fbstring &whatArg)
        : std::out_of_range{
              "missing helper parameter: '" + whatArg.toStdString() + "'"}
    {
    }
};

/**
 * An exception reporting a bad parameter value for helper creation.
 */
class BadParameterException : public std::invalid_argument {
public:
    /**
     * Constructor.
     * @param whatArg Name of the bad parameter.
     * @param value Value of the bad parameter.
     */
    BadParameterException(
        const folly::fbstring &whatArg, const folly::fbstring &value)
        : std::invalid_argument{"bad helper parameter value: '" +
              whatArg.toStdString() + "' -> '" + value.toStdString() + "'"}
    {
    }
};

/**
 * Retrieves a value from a key-value map, typecasted to a specific type.
 * @tparam Ret the type to convert the value to.
 * @param params The key-value map.
 * @param key Key of the value in the key-value map.
 * @returns Value indexed by the @c key, typecasted to @c Ret.
 */
template <typename Ret = folly::fbstring>
Ret getParam(const Params &params, const folly::fbstring &key)
{
    try {
        return boost::lexical_cast<Ret>(params.at(key));
    }
    catch (const std::out_of_range &) {
        throw MissingParameterException{key};
    }
    catch (const boost::bad_lexical_cast) {
        throw BadParameterException{key, params.at(key)};
    }
}

/**
 * @copydoc getParam(params, key)
 * @tparam Def the type of the default value (inferred).
 * @param def The default value to use if the key is not found in the map.
 */
template <typename Ret = folly::fbstring, typename Def>
Ret getParam(const Params &params, const folly::fbstring &key, Def &&def)
{
    try {
        auto param = params.find(key);
        if (param != params.end())
            return boost::lexical_cast<Ret>(param->second);

        return std::forward<Def>(def);
    }
    catch (const boost::bad_lexical_cast) {
        throw BadParameterException{key, params.at(key)};
    }
}

template <>
folly::fbstring getParam<folly::fbstring>(
    const Params &params, const folly::fbstring &key);

template <>
folly::fbstring getParam<folly::fbstring, folly::fbstring>(
    const Params &params, const folly::fbstring &key, folly::fbstring &&def);

template <>
StoragePathType getParam<StoragePathType>(
    const Params &params, const folly::fbstring &key);

/**
 * @c StorageHelperFactory is responsible for creating a helper instance from
 * generic parameter representation.
 */
class StorageHelperFactory {
public:
    virtual ~StorageHelperFactory() = default;

    /**
     * Returns the type name of the helper (e.g. posix)
     */
    virtual folly::fbstring name() const = 0;

    /**
     * Creates an instance of @c StorageHelper .
     * @param parameters Parameters for helper creation.
     * @returns A new instance of @c StorageHelper .
     */
    virtual StorageHelperPtr createStorageHelper(const Params &parameters,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER) = 0;

    /**
     * Returns a list of helper specific parameters which can be overriden on
     * the client side.
     */
    virtual const std::vector<folly::fbstring> overridableParams() const
    {
        return {};
    };

    /**
     * This method allows to create a storage helper by taking into account
     * any overriden helper parameter values provided by the user.
     * @param parameters Common parameters from the Oneprovider
     * @param overrideParameters Client specific parameters, which can override
     *        the common values, if allowed by helper
     */
    StorageHelperPtr createStorageHelperWithOverride(Params parameters,
        const Params &overrideParameters,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER)
    {
        LOG_FCALL() << LOG_FARGM(overrideParameters);

        const auto &overridable = overridableParams();

        for (const auto &p : overrideParameters) {
            const auto &parameterName = p.first;
            if (std::find(overridable.cbegin(), overridable.cend(),
                    parameterName) != overridable.end()) {
                LOG_DBG(1) << "Overriding " << name() << " storage parameter "
                           << parameterName << " with value " << p.second;

                parameters[parameterName] = p.second;
            }
            else
                LOG(WARNING) << "Storage helper " << name() << " parameter "
                             << parameterName << " cannot be overriden";
        }

        return createStorageHelper(parameters, executionContext);
    }
};

/**
 * @c FileHandle represents a single file "opening".
 */
class FileHandle {
public:
    /**
     * Constructor.
     * @param fileId Helper-specific ID of the open file.
     */
    FileHandle(folly::fbstring fileId, std::shared_ptr<StorageHelper> helper)
        : FileHandle{std::move(fileId), {}, std::move(helper)}
    {
    }

    /**
     * @copydoc FileHandle(fileId)
     * @param openParams Additional parameters associated with the handle.
     */
    FileHandle(folly::fbstring fileId, Params openParams,
        std::shared_ptr<StorageHelper> helper)
        : m_fileId{std::move(fileId)}
        , m_openParams{std::move(openParams)}
        , m_helper{std::move(helper)}
    {
    }

    virtual ~FileHandle() = default;

    std::shared_ptr<StorageHelper> helper() { return m_helper; }

    virtual folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) = 0;

    virtual folly::Future<folly::IOBufQueue> read(const off_t offset,
        const std::size_t size, const std::size_t continuousBlock)
    {
        return read(offset, size);
    }

    virtual folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb) = 0;

    virtual folly::Future<std::size_t> multiwrite(
        folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>>
            buffs);

    virtual folly::Future<folly::Unit> release() { return folly::makeFuture(); }

    virtual folly::Future<folly::Unit> flush() { return folly::makeFuture(); }

    virtual folly::Future<folly::Unit> fsync(bool isDataSync)
    {
        return folly::makeFuture();
    }

    virtual const Timeout &timeout() = 0;

    virtual bool needsDataConsistencyCheck() { return false; }

    virtual folly::fbstring fileId() const { return m_fileId; }

    virtual std::size_t wouldPrefetch(
        const off_t offset, const std::size_t size)
    {
        return 0;
    }

    virtual folly::Future<folly::Unit> flushUnderlying() { return flush(); }

    virtual bool isConcurrencyEnabled() const { return false; }

    /**
     * Updates the underlying helper storage parameters. Override in case the
     * file handle for a specific helper needs to be updated after this
     * operation.
     *
     * @param params Storage helper parameters
     */
    virtual folly::Future<folly::Unit> refreshHelperParams(
        std::shared_ptr<StorageHelperParams> params);

protected:
    folly::fbstring m_fileId;
    Params m_openParams;
    std::shared_ptr<StorageHelper> m_helper;
};

class StorageHelperParams {
public:
    virtual ~StorageHelperParams() = default;

    static std::shared_ptr<StorageHelperParams> create(
        const folly::fbstring &name, const Params &params);

    virtual void initializeFromParams(const Params &parameters)
    {
        m_timeout = Timeout{getParam<std::size_t>(
            parameters, "timeout", ASYNC_OPS_TIMEOUT.count())};

        auto storagePathTypeString =
            getParam<std::string>(parameters, "storagePathType", "canonical");
        if (storagePathTypeString == "canonical")
            m_storagePathType = StoragePathType::CANONICAL;
        else if (storagePathTypeString == "flat")
            m_storagePathType = StoragePathType::FLAT;
        else
            throw BadParameterException{
                "storagePathType", storagePathTypeString};
    }

    const Timeout &timeout() const { return m_timeout; }

    StoragePathType storagePathType() const { return m_storagePathType; }

private:
    Timeout m_timeout;
    StoragePathType m_storagePathType;
};

/**
 * The StorageHelper interface.
 * Base class of all storage helpers. Unifies their interface.
 * All callback have their equivalent in FUSE API and should be used in that
 * matter.
 */
class StorageHelper {
public:
    using StorageHelperParamsPromise =
        folly::SharedPromise<std::shared_ptr<StorageHelperParams>>;

    StorageHelper(
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER)
        : m_params{std::make_shared<StorageHelperParamsPromise>()}
        , m_executionContext{executionContext}
    {
    }

    virtual ~StorageHelper() = default;

    virtual folly::fbstring name() const = 0;

    virtual folly::Future<struct stat> getattr(const folly::fbstring &fileId)
    {
        return folly::makeFuture<struct stat>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask)
    {
        return folly::makeFuture();
    }

    virtual folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId)
    {
        return folly::makeFuture<folly::fbstring>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count)
    {
        return folly::makeFuture<folly::fbvector<folly::fbstring>>(
            std::system_error{
                std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> chown(
        const folly::fbstring &fileId, const uid_t uid, const gid_t gid)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const FlagsSet &flags, const Params &openParams)
    {
        return open(fileId, flagsToMask(flags), openParams);
    }

    virtual folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) = 0;

    virtual folly::Future<ListObjectsResult> listobjects(
        const folly::fbstring &prefix, const folly::fbstring &marker,
        const off_t offset, const size_t count)
    {
        return folly::makeFuture<ListObjectsResult>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> multipartCopy(
        const folly::fbstring &sourceKey, const folly::fbstring &destinationKey)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name)
    {
        return folly::makeFuture<folly::fbstring>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> setxattr(const folly::fbstring &uuid,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::Unit> removexattr(
        const folly::fbstring &uuid, const folly::fbstring &name)
    {
        return folly::makeFuture<folly::Unit>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    }

    virtual folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &uuid)
    {
        return folly::makeFuture<folly::fbvector<folly::fbstring>>(
            std::system_error{
                std::make_error_code(std::errc::function_not_supported)});
    }

    /**
     * Returns a future to an instance of storage helper parameters.
     * It allows for overriding for special helpers such as @c BufferAgent.
     */
    virtual folly::Future<std::shared_ptr<StorageHelperParams>> params() const
    {
        std::lock_guard<std::mutex> m_lock{m_paramsMutex};
        return m_params->getFuture();
    }

    /**
     * Updates the helper parameters by replacing the parameters promise
     * stored in storage helper with a new one. In this way requests already
     * created will be allowed to execute with the old set of parameters
     * while all new requests will use the new set.
     *
     * @param params Shared instance of @c StorageHelperParams
     */
    virtual folly::Future<folly::Unit> refreshParams(
        std::shared_ptr<StorageHelperParams> params)
    {
        return folly::via(
            executor().get(), [this, params = std::move(params)]() {
                invalidateParams()->setValue(std::move(params));
            });
    }

    virtual const Timeout &timeout() { return params().get()->timeout(); }

    virtual StoragePathType storagePathType() const
    {
        return params().get()->storagePathType();
    }

    bool isFlat() const { return storagePathType() == StoragePathType::FLAT; }

    virtual std::size_t blockSize() const noexcept { return 0; }

    virtual bool isObjectStorage() const noexcept { return false; }

    virtual std::shared_ptr<folly::Executor> executor() { return {}; };

    ExecutionContext executionContext() const { return m_executionContext; }

protected:
    std::shared_ptr<StorageHelperParamsPromise> invalidateParams()
    {
        std::lock_guard<std::mutex> m_lock{m_paramsMutex};
        m_params = std::make_shared<StorageHelperParamsPromise>();
        return m_params;
    };

private:
    // Pointer to a promise of a shared pointers to helper parameters
    // This allows the parameters to be safely updated as with
    // existing handles and parallel read/write operations.
    std::shared_ptr<StorageHelperParamsPromise> m_params;
    mutable std::mutex m_paramsMutex;
    const ExecutionContext m_executionContext;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_STORAGE_HELPER_H
