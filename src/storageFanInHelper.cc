/**
 * @file storageFanInHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "storageFanInHelper.h"

namespace one {
namespace helpers {

StorageFanInHelper::StorageFanInHelper(std::vector<StorageHelperPtr> storages,
    std::shared_ptr<folly::Executor> executor,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_storages{std::move(storages)}
    , m_executor{std::move(executor)}
{
}

folly::fbstring StorageFanInHelper::name() const
{
    return m_storages.front()->name();
}

folly::Future<struct stat> StorageFanInHelper::getattr(
    const folly::fbstring &fileId)
{
    std::vector<folly::Future<struct stat>> futs;
    for (auto &storage : m_storages) {
        futs.emplace_back(storage->getattr(fileId));
    }

    return folly::collectAll(futs)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<struct stat>> &&res) {
            std::vector<struct stat> results;
            for (const auto &tryStat : res) {
                if (tryStat.hasValue()) {
                    results.push_back(tryStat.value());
                }
            }
            return results;
        })
        .thenValue([](std::vector<struct stat> &&results) {
            if (results.empty())
                return makeFuturePosixException<struct stat>(ENOENT);

            struct stat statIt = results[0];
            for (const auto &s : results) {
                if (s.st_mtim.tv_sec > statIt.st_mtim.tv_sec)
                    statIt.st_mtim = s.st_mtim;
                if (s.st_atim.tv_sec > statIt.st_atim.tv_sec)
                    statIt.st_atim = s.st_atim;
                if (s.st_ctim.tv_sec < statIt.st_ctim.tv_sec)
                    statIt.st_ctim = s.st_ctim;
            }

            return folly::makeFuture<struct stat>(std::move(statIt));
        });
}

folly::Future<folly::Unit> StorageFanInHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    std::vector<folly::Future<folly::Unit>> futs;
    for (auto &storage : m_storages) {
        futs.emplace_back(storage->access(fileId, mask));
    }

    return folly::collectAll(futs)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<folly::Unit>> &&res) {
            std::vector<struct stat> results;
            for (const auto &tryAccess : res) {
                if (tryAccess.hasValue()) {
                    return folly::makeFuture();
                }
            }

            return folly::makeFuture(res.front().value());
        });
}

folly::Future<folly::fbstring> StorageFanInHelper::readlink(
    const folly::fbstring &fileId)
{
    std::vector<folly::Future<folly::fbstring>> futs;
    for (auto &storage : m_storages) {
        futs.emplace_back(storage->readlink(fileId));
    }

    return folly::collectAll(futs)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<folly::fbstring>> &&res) {
            for (const auto &tryReadlink : res) {
                if (tryReadlink.hasValue()) {
                    folly::fbstring v = tryReadlink.value();
                    return folly::makeFuture<folly::fbstring>(std::move(v));
                }
            }

            folly::fbstring v = res.front().value();
            return folly::makeFuture<folly::fbstring>(std::move(v));
        });
}

folly::Future<folly::fbvector<folly::fbstring>> StorageFanInHelper::readdir(
    const folly::fbstring &fileId, const off_t offset, const std::size_t count)
{
    constexpr auto kReaddirStep = 64'000U;

    folly::fbvector<folly::fbstring> results;
    folly::fbvector<folly::fbstring> result;
    // The set is to make sure the result vector is unique
    std::set<folly::fbstring> resultSet;

    for (auto storage : m_storages) {
        auto entries = storage->readdir(fileId, 0, kReaddirStep).getTry();

        if (entries.hasException())
            // This storage does not have this directory
            continue;

        for (const auto &entry : entries.value()) {
            if (resultSet.find(entry) == resultSet.end()) {
                resultSet.insert(entry);
                results.push_back(entry);
            }
        }

        if (results.size() >= static_cast<size_t>(offset) + count) {
            break;
        }
    }

    if (static_cast<size_t>(offset) > results.size())
        return result;

    auto beginIt = results.begin();
    std::advance(beginIt, offset);
    auto endIt = beginIt;
    std::advance(endIt, std::min(count, results.size() - offset));
    std::copy(beginIt, endIt, std::back_inserter(result));

    return result;
}

folly::Future<FileHandlePtr> StorageFanInHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    std::vector<folly::Future<FileHandlePtr>> futs;
    for (auto &storage : m_storages) {
        futs.emplace_back(storage->open(fileId, flags, openParams));
    }

    return folly::collectAll(futs)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<FileHandlePtr>> &&res) {
            FileHandlePtr result;
            for (const auto &tryOpen : res) {
                if (tryOpen.hasValue()) {
                    if (!result)
                        result = std::move(tryOpen.value());
                    else
                        tryOpen.value()->release();
                }
            }

            if (result)
                return folly::makeFuture<FileHandlePtr>(std::move(result));

            FileHandlePtr v = res.front().value();
            return folly::makeFuture<FileHandlePtr>(std::move(v));
        });
}

folly::Future<folly::fbstring> StorageFanInHelper::getxattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    std::vector<folly::Future<folly::fbstring>> futs;
    for (auto &storage : m_storages) {
        futs.emplace_back(storage->getxattr(uuid, name));
    }

    return folly::collectAll(futs)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<folly::fbstring>> &&res) {
            for (const auto &tryGetxattr : res) {
                if (tryGetxattr.hasValue()) {
                    folly::fbstring v = tryGetxattr.value();
                    return folly::makeFuture<folly::fbstring>(std::move(v));
                }
            }
            folly::fbstring v = res.front().value();
            return folly::makeFuture<folly::fbstring>(std::move(v));
        });
}

folly::Future<folly::fbvector<folly::fbstring>> StorageFanInHelper::listxattr(
    const folly::fbstring &uuid)
{
    std::vector<folly::Future<folly::fbvector<folly::fbstring>>> futs;
    for (auto &storage : m_storages) {
        futs.emplace_back(storage->listxattr(uuid));
    }

    return folly::collectAll(futs)
        .via(m_executor.get())
        .thenValue([](std::vector<folly::Try<folly::fbvector<folly::fbstring>>>
                           &&res) {
            std::vector<struct stat> results;
            for (const auto &tryListxattr : res) {
                if (tryListxattr.hasValue()) {
                    folly::fbvector<folly::fbstring> v = tryListxattr.value();
                    return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                        std::move(v));
                }
            }
            folly::fbvector<folly::fbstring> v = res.front().value();
            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(v));
        });
}

bool StorageFanInHelper::isObjectStorage() const { return false; }
} // namespace helpers
} // namespace one
