/**
 * @file storageHelper.cc
 * @author Michal Wrona
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/storageHelper.h"
#include "fuseOperations.h"
#include "helpers/logging.h"
#include "posixHelperParams.h"
#include "webDAVHelperParams.h"
#if WITH_XROOTD
#include "xrootdHelperParams.h"
#endif

#include <folly/futures/Future.h>

#include <sys/stat.h>

namespace {

using one::helpers::Flag;

const std::unordered_map<Flag, int, one::helpers::FlagHash> g_flagTranslation{
    {Flag::NONBLOCK, O_NONBLOCK}, {Flag::APPEND, O_APPEND},
    {Flag::ASYNC, O_ASYNC}, {Flag::FSYNC, O_FSYNC},
    {Flag::NOFOLLOW, O_NOFOLLOW}, {Flag::CREAT, O_CREAT},
    {Flag::TRUNC, O_TRUNC}, {Flag::EXCL, O_EXCL}, {Flag::RDONLY, O_RDONLY},
    {Flag::WRONLY, O_WRONLY}, {Flag::RDWR, O_RDWR}, {Flag::IFREG, S_IFREG},
    {Flag::IFCHR, S_IFCHR}, {Flag::IFBLK, S_IFBLK}, {Flag::IFIFO, S_IFIFO},
    {Flag::IFSOCK, S_IFSOCK}};

const std::unordered_map<int, Flag> g_maskTranslation{
    {O_NONBLOCK, Flag::NONBLOCK}, {O_APPEND, Flag::APPEND},
    {O_ASYNC, Flag::ASYNC}, {O_FSYNC, Flag::FSYNC},
    {O_NOFOLLOW, Flag::NOFOLLOW}, {O_CREAT, Flag::CREAT},
    {O_TRUNC, Flag::TRUNC}, {O_EXCL, Flag::EXCL}, {O_RDONLY, Flag::RDONLY},
    {O_WRONLY, Flag::WRONLY}, {O_RDWR, Flag::RDWR}, {S_IFREG, Flag::IFREG},
    {S_IFCHR, Flag::IFCHR}, {S_IFBLK, Flag::IFBLK}, {S_IFIFO, Flag::IFIFO},
    {S_IFSOCK, Flag::IFSOCK}};

} // namespace

namespace one {
namespace helpers {

template <>
folly::fbstring getParam<folly::fbstring>(
    const Params &params, const folly::fbstring &key)
{
    try {
        return params.at(key);
    }
    catch (const std::out_of_range &) {
        throw MissingParameterException{key};
    }
}

template <>
folly::fbstring getParam<folly::fbstring, folly::fbstring>(
    const Params &params, const folly::fbstring &key, folly::fbstring &&def)
{
    auto param = params.find(key);
    if (param != params.end())
        return param->second;

    return std::forward<folly::fbstring>(def);
}

template <>
bool getParam<bool, bool>(
    const Params &params, const folly::fbstring &key, bool &&def)
{
    auto param = params.find(key);
    if (param != params.end())
        return param->second == "true";

    return def;
}

template <>
StoragePathType getParam<StoragePathType>(
    const Params &params, const folly::fbstring &key)
{
    try {
        const auto &param = params.at(key);
        if (param == "canonical")
            return StoragePathType::CANONICAL;

        if (param == "flat")
            return StoragePathType::FLAT;

        throw BadParameterException{key, param};
    }
    catch (const std::out_of_range &) {
        throw MissingParameterException{key};
    }
}

mode_t parsePosixPermissions(folly::fbstring p)
{
    if ((p.length() != 3) && (p.length() != 4)) {
        throw std::invalid_argument(
            "Invalid permission string: " + p.toStdString());
    }

    if (p.length() == 3)
        p = "0" + p;

    mode_t result = 0;

    for (auto i = 0u; i < 4; i++) {
        result += (p[i] - '0') << (3 * (3 - i));
    }
    return result;
}

std::shared_ptr<StorageHelperParams> StorageHelperParams::create(
    const folly::fbstring &name, const Params &params)
{
    if (name == POSIX_HELPER_NAME) {
        return PosixHelperParams::create(params);
    }
#if WITH_WEBDAV
    if (name == WEBDAV_HELPER_NAME) {
        return WebDAVHelperParams::create(params);
    }
#endif
#if WITH_XROOTD
    if (name == XROOTD_HELPER_NAME) {
        return XRootDHelperParams::create(params);
    }
#endif

    throw std::invalid_argument(
        "Unsupported storage helper type: " + name.toStdString());
}

int flagsToMask(const FlagsSet &flags)
{
    int value = 0;

    for (auto flag : flags) {
        auto searchResult = g_flagTranslation.find(flag);
        assert(searchResult != g_flagTranslation.end());
        value |= searchResult->second;
    }
    return value;
}

FlagsSet maskToFlags(int mask)
{
    FlagsSet flags;

    // get permission flags
    flags.insert(g_maskTranslation.at(mask & O_ACCMODE));

    // get other flags
    for (auto entry : g_maskTranslation) {
        auto entry_mask = entry.first;
        auto entry_flag = entry.second;

        if (entry_flag != Flag::RDONLY && entry_flag != Flag::WRONLY &&
            entry_flag != Flag::RDWR && (entry_mask & mask) == entry_mask)
            flags.insert(g_maskTranslation.at(entry_mask));
    }

    return flags;
}

std::vector<folly::fbstring> StorageHelperFactory::overridableParams() const
{
    return {};
}

StorageHelperPtr StorageHelperFactory::createStorageHelperWithOverride(
    Params parameters, const Params &overrideParameters,
    ExecutionContext executionContext)
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

FileHandle::FileHandle(
    folly::fbstring fileId, std::shared_ptr<StorageHelper> helper)
    : FileHandle{std::move(fileId), {}, std::move(helper)}
{
}

FileHandle::FileHandle(folly::fbstring fileId, Params openParams,
    std::shared_ptr<StorageHelper> helper)
    : m_fileId{std::move(fileId)}
    , m_openParams{std::move(openParams)}
    , m_helper{std::move(helper)}
{
}

std::shared_ptr<StorageHelper> FileHandle::helper() { return m_helper; }

folly::Future<folly::IOBufQueue> FileHandle::read(const off_t offset,
    const std::size_t size, const std::size_t /*continuousBlock*/)
{
    return read(offset, size);
}

folly::Future<std::size_t> multiwrite(
    folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>> buffs);

folly::Future<folly::Unit> FileHandle::release() { return folly::makeFuture(); }

folly::Future<folly::Unit> FileHandle::flush() { return folly::makeFuture(); }

folly::Future<folly::Unit> FileHandle::fsync(bool /*isDataSync*/)
{
    return folly::makeFuture();
}

bool FileHandle::needsDataConsistencyCheck() { return false; }

folly::fbstring FileHandle::fileId() const { return m_fileId; }

std::size_t FileHandle::wouldPrefetch(
    const off_t /*offset*/, const std::size_t /*size*/)
{
    return 0;
}

folly::Future<folly::Unit> FileHandle::flushUnderlying() { return flush(); }

bool FileHandle::isConcurrencyEnabled() const { return false; }

folly::Future<folly::Unit> FileHandle::refreshHelperParams(
    std::shared_ptr<StorageHelperParams> params)
{
    return m_helper->refreshParams(std::move(params));
}

folly::Future<std::size_t> FileHandle::multiwrite(
    folly::fbvector<std::tuple<off_t, folly::IOBufQueue, WriteCallback>> buffs)
{
    LOG_FCALL();

    auto future = folly::makeFuture<std::size_t>(0);

    std::size_t shouldHaveWrittenSoFar = 0;

    for (auto &buf : buffs) {
        auto size = std::get<1>(buf).chainLength();
        const auto shouldHaveWrittenAfter = shouldHaveWrittenSoFar + size;

        future = future.then(
            [this, shouldHaveWrittenSoFar, size, buf = std::move(buf)](
                const std::size_t wroteSoFar) mutable {
                if (shouldHaveWrittenSoFar < wroteSoFar)
                    return folly::makeFuture(wroteSoFar);

                using one::logging::log_timer;
                using one::logging::csv::log;
                using one::logging::csv::read_write_perf;

                log_timer<> timer;
                auto offset = std::get<0>(buf);
                return write(offset, std::move(std::get<1>(buf)),
                    std::move(std::get<2>(buf)))
                    .then([wroteSoFar, offset, size, timer, fileId = m_fileId](
                              const std::size_t wrote) mutable {
                        log<read_write_perf>(fileId, "FileHandle", "write",
                            offset, size, timer.stop());

                        return wroteSoFar + wrote;
                    });
            });

        shouldHaveWrittenSoFar = shouldHaveWrittenAfter;
    }

    return future;
}

void FileHandle::setOverrideParams(const Params &params)
{
    m_paramsOverride = params;
}

folly::Future<struct stat> StorageHelper::getattr(
    const folly::fbstring & /*fileId*/)
{
    return folly::makeFuture<struct stat>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::access(
    const folly::fbstring & /*fileId*/, const int /*mask*/)
{
    return folly::makeFuture();
}

folly::Future<folly::fbstring> StorageHelper::readlink(
    const folly::fbstring & /*fileId*/)
{
    return folly::makeFuture<folly::fbstring>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::fbvector<folly::fbstring>> StorageHelper::readdir(
    const folly::fbstring & /*fileId*/, const off_t /*offset*/,
    const std::size_t /*count*/)
{
    return folly::makeFuture<folly::fbvector<folly::fbstring>>(
        std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::mknod(
    const folly::fbstring & /*fileId*/, const mode_t /*mode*/,
    const FlagsSet & /*flags*/, const dev_t /*rdev*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::mkdir(
    const folly::fbstring & /*fileId*/, const mode_t /*mode*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::unlink(
    const folly::fbstring & /*fileId*/, const size_t /*currentSize*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::rmdir(
    const folly::fbstring & /*fileId*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::symlink(
    const folly::fbstring & /*from*/, const folly::fbstring & /*to*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::rename(
    const folly::fbstring & /*from*/, const folly::fbstring & /*to*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::link(
    const folly::fbstring & /*from*/, const folly::fbstring & /*to*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::chmod(
    const folly::fbstring & /*fileId*/, const mode_t /*mode*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::chown(
    const folly::fbstring & /*fileId*/, const uid_t /*uid*/,
    const gid_t /*gid*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::truncate(
    const folly::fbstring & /*fileId*/, const off_t /*size*/,
    const size_t /*currentSize*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<FileHandlePtr> StorageHelper::open(const folly::fbstring &fileId,
    const FlagsSet &flags, const Params &openParams)
{
    return open(fileId, flagsToMask(flags), openParams, {});
}

folly::Future<FileHandlePtr> StorageHelper::open(const folly::fbstring &fileId,
    const FlagsSet &flags, const Params &openParams,
    const Params &helperOverrideParams)
{
    return open(fileId, flagsToMask(flags), openParams, helperOverrideParams);
}

folly::Future<FileHandlePtr> StorageHelper::open(const folly::fbstring &fileId,
    const int flags, const Params &openParams,
    const Params &helperOverrideParams)
{
    validateHandleOverrideParams(helperOverrideParams);

    return open(fileId, flags, openParams)
        .then([helperOverrideParams](FileHandlePtr &&fh) {
            fh->setOverrideParams(helperOverrideParams);
            return std::move(fh);
        });
}

folly::Future<ListObjectsResult> StorageHelper::listobjects(
    const folly::fbstring & /*prefix*/, const folly::fbstring & /*marker*/,
    const off_t /*offset*/, const size_t /*count*/)
{
    return folly::makeFuture<ListObjectsResult>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::multipartCopy(
    const folly::fbstring & /*sourceKey*/,
    const folly::fbstring & /*destinationKey*/, const std::size_t /*blockSize*/,
    const std::size_t /*size*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::fbstring> StorageHelper::getxattr(
    const folly::fbstring & /*uuid*/, const folly::fbstring & /*name*/)
{
    return folly::makeFuture<folly::fbstring>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::setxattr(
    const folly::fbstring & /*uuid*/, const folly::fbstring & /*name*/,
    const folly::fbstring & /*value*/, bool /*create*/, bool /*replace*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::removexattr(
    const folly::fbstring & /*uuid*/, const folly::fbstring & /*name*/)
{
    return folly::makeFuture<folly::Unit>(std::system_error{
        std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::fbvector<folly::fbstring>> StorageHelper::listxattr(
    const folly::fbstring & /*uuid*/)
{
    return folly::makeFuture<folly::fbvector<folly::fbstring>>(
        std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
}

folly::Future<folly::Unit> StorageHelper::loadBuffer(
    const folly::fbstring & /*fileId*/, const std::size_t /*size*/)
{
    return {};
}

folly::Future<folly::Unit> StorageHelper::flushBuffer(
    const folly::fbstring & /*fileId*/, const std::size_t /*size*/)
{
    return {};
}

folly::Future<std::size_t> StorageHelper::blockSizeForPath(
    const folly::fbstring & /*fileId*/)
{
    return blockSize();
}

folly::Future<std::shared_ptr<StorageHelperParams>>
StorageHelper::params() const
{
    std::lock_guard<std::mutex> m_lock{m_paramsMutex};
    return m_params->getFuture();
}

folly::Future<folly::Unit> StorageHelper::refreshParams(
    std::shared_ptr<StorageHelperParams> params)
{
    return folly::via(executor().get(), [this, params = std::move(params)]() {
        invalidateParams()->setValue(params);
    });
}

void StorageHelper::validateHandleOverrideParams(const Params &params)
{
    const auto &overridableParams = handleOverridableParams();
    for (const auto &p : params) {
        if (std::find(overridableParams.begin(), overridableParams.end(),
                p.first) == overridableParams.cend())
            throw BadParameterException{p.first, p.second};
    }
}
std::vector<folly::fbstring> StorageHelper::handleOverridableParams() const
{
    return {};
}

const Timeout &StorageHelper::timeout() { return params().get()->timeout(); }

StoragePathType StorageHelper::storagePathType() const
{
    return params().get()->storagePathType();
}

bool StorageHelper::isFlat() const
{
    return storagePathType() == StoragePathType::FLAT;
}

std::size_t StorageHelper::blockSize() const noexcept { return 0; }

bool StorageHelper::isObjectStorage() const noexcept { return false; }

std::shared_ptr<folly::Executor> StorageHelper::executor() { return {}; }

ExecutionContext StorageHelper::executionContext() const
{
    return m_executionContext;
}

std::shared_ptr<StorageHelper::StorageHelperParamsPromise>
StorageHelper::invalidateParams()
{
    std::lock_guard<std::mutex> m_lock{m_paramsMutex};
    m_params = std::make_shared<StorageHelper::StorageHelperParamsPromise>();
    return m_params;
}

} // namespace helpers
} // namespace one
