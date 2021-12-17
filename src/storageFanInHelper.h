/**
 * @file storageFanInHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include "helpers/logging.h"

#include <vector>

namespace one {
namespace helpers {

class StorageFanInHelper
    : public StorageHelper,
      public std::enable_shared_from_this<StorageFanInHelper> {
public:
    /**
     * Constructor.
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    StorageFanInHelper(std::vector<StorageHelperPtr> storages,
        std::shared_ptr<folly::Executor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    /**
     * Destructor.
     * Closes connection to FanIn storage cluster and destroys internal
     * context object.
     */
    virtual ~StorageFanInHelper() = default;

    folly::fbstring name() const override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override;

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count) override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId) override;

    bool isObjectStorage() const override;

private:
    std::vector<StorageHelperPtr> m_storages;

    std::shared_ptr<folly::Executor> m_executor;
};

/**
 * An implementation of @c StorageHelperFactory for FanIn storage
 * helper.
 */
class StorageFanInHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    StorageFanInHelperFactory() { LOG_FCALL(); }

    virtual folly::fbstring name() const override
    {
        return STORAGE_FANIN_HELPER_NAME;
    }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"mountPoint", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params & /*parameters*/,
        ExecutionContext executionContext =
            ExecutionContext::ONEPROVIDER) override
    {
        std::vector<StorageHelperPtr> storages;
        return std::make_shared<StorageFanInHelper>(std::move(storages),
            storages.front()->executor(), executionContext);
    }
};

} // namespace helpers
} // namespace one
