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
 * The class implements reference counting for cached helpers - each
 * getStorageHelper call increases the reference count, and releaseStorageHelper
 * decreases it. When the count reaches zero, the helper is removed from cache.
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
     * returns the cached instance and increases its reference count.
     * Otherwise creates a new one, caches it and sets reference count to 1.
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
     * returns the cached instance and increases its reference count.
     * Otherwise creates a new one, caches it and sets reference count to 1.
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
            accessor->second.first =
                m_creator->getStorageHelper(type, args, buffered);
            accessor->second.first->id(key);
            accessor->second.second = 1; // Initialize reference count
        }
        else {
            // Key was in cache, increment reference count
            accessor->second.second++;
        }

        return accessor->second.first;
    }

    /**
     * Release a storage helper instance.
     * Decrements the reference count for the helper matching the given
     * arguments. If the reference count reaches zero, removes the helper from
     * cache.
     * @return true if the helper was found and released, false otherwise
     */
    bool releaseStorageHelper(const folly::fbstring &id)
    {
        const auto key = id.toStdString();

        typename CacheMap::accessor accessor;
        if (m_cache.find(accessor, key)) {
            if (--accessor->second.second == 0) {
                m_cache.erase(accessor);
            }
            return true;
        }
        return false;
    }

    bool releaseStorageHelper(StorageHelper *helper)
    {
        if (helper == nullptr) {
            return false;
        }

        return releaseStorageHelper(helper->id());
    }

private:
    using CacheKey = std::string;
    // Pair of storage helper and its reference count
    using CacheValue = std::pair<std::shared_ptr<StorageHelper>, std::size_t>;
    using CacheMap = tbb::concurrent_hash_map<CacheKey, CacheValue>;

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