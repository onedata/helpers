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

    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
    for (auto &buf : buffs) {
        auto size = std::get<1>(buf).chainLength();
        const auto shouldHaveWrittenAfter = shouldHaveWrittenSoFar + size;

        // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
        future = future.then(
            [this, shouldHaveWrittenSoFar, size, buf = std::move(buf)](
                const std::size_t wroteSoFar) mutable {
                // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
                if (shouldHaveWrittenSoFar < wroteSoFar)
                    return folly::makeFuture(wroteSoFar);

                using one::logging::log_timer;
                using one::logging::csv::log;
                using one::logging::csv::read_write_perf;

                log_timer<> timer;
                auto offset = std::get<0>(buf);
                // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
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

} // namespace helpers
} // namespace one
