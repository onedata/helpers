#ifndef HELPERS_CACHING_STORAGE_HELPER_CREATOR_H
#define HELPERS_CACHING_STORAGE_HELPER_CREATOR_H

#include "storageHelper.h"
#include "storageHelperCreator.h"

#include <folly/FBString.h>
#include <tbb/concurrent_hash_map.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace one {
namespace helpers {

/**
 * A wrapper around StorageHelperCreator that provides caching of created
 * storage helpers. For the same combination of arguments and buffered flag, it
 * will return the same storage helper instance instead of creating a new one.
 */
template <typename CommunicatorT> class CachingStorageHelperCreator {
public:
    explicit CachingStorageHelperCreator(
        std::unique_ptr<StorageHelperCreator<CommunicatorT>> creator)
        : m_creator{std::move(creator)}
    {
    }

    /**
     * Get or create a storage helper for the given arguments.
     * If a storage helper was previously created with the same arguments,
     * returns the cached instance. Otherwise creates a new one and caches it.
     */
    std::shared_ptr<StorageHelper> getStorageHelper(
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered)
    {
        return getStorageHelper(args.at("type"), args, buffered);
    }

    /**
     * Get or create a storage helper for the given arguments.
     * If a storage helper was previously created with the same arguments,
     * returns the cached instance. Otherwise creates a new one and caches it.
     */
    std::shared_ptr<StorageHelper> getStorageHelper(const folly::fbstring &type,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered)
    {
        // Create a cache key from args and buffered flag
        auto key = createCacheKey(type, args, buffered);

        typename CacheMap::accessor accessor;
        if (m_cache.insert(accessor, key)) {
            // Key wasn't in cache, create new storage helper
            accessor->second = m_creator->getStorageHelper(args, buffered);
        }

        return accessor->second;
    }

private:
    using CacheKey = std::string;
    using CacheMap =
        tbb::concurrent_hash_map<CacheKey, std::shared_ptr<StorageHelper>>;

    /**
     * Creates a unique cache key from storage helper arguments and buffered
     * flag.
     */
    static CacheKey createCacheKey(const folly::fbstring &type,
        const std::unordered_map<folly::fbstring, folly::fbstring> &args,
        bool buffered)
    {
        folly::fbstring key = type + ";";

        // Add all args to key in sorted order for consistency
        std::vector<std::pair<folly::fbstring, folly::fbstring>> sortedArgs(
            args.begin(), args.end());
        std::sort(sortedArgs.begin(), sortedArgs.end());

        for (const auto &arg : sortedArgs) {
            key += arg.first + "=" + arg.second + ";";
        }

        // Add buffered flag
        key += "buffered=" + std::to_string(buffered);

        return key.toStdString();
    }

    std::unique_ptr<StorageHelperCreator<CommunicatorT>> m_creator;
    CacheMap m_cache;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_CACHING_STORAGE_HELPER_CREATOR_H