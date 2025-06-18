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

const auto kHelperCacheDefaultExpirySeconds{300};

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
        std::unique_ptr<StorageHelperCreator<CommunicatorT>> creator,
        std::chrono::seconds expirySeconds =
            std::chrono::seconds{kHelperCacheDefaultExpirySeconds})
        : m_creator{std::move(creator)}
        , m_expirySeconds{expirySeconds}
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
        auto now = std::chrono::steady_clock::now();

        typename CacheMap::accessor accessor;
        if (m_cache.insert(accessor, key)) {
            // Key wasn't in cache, create new storage helper
            accessor->second.first =
                m_creator->getStorageHelper(type, args, buffered);
            accessor->second.first->id(key);
            accessor->second.second = now; // Initialize reference count
        }
        else {
            if (!accessor->second.first) {
                accessor->second.first =
                    m_creator->getStorageHelper(type, args, buffered);
                accessor->second.first->id(key);
            }
            accessor->second.second = now;
        }

        return accessor->second.first;
    }

    bool clean()
    {
        bool removed{false};

        auto now = std::chrono::steady_clock::now();
        for (typename CacheMap::iterator it = m_cache.begin();
             it != m_cache.end(); it++) {
            const auto &lastAccess = it->second.second;
            if (now - lastAccess > m_expirySeconds &&
                it->second.first.use_count() == 1) {
                it->second.first.reset();
                removed = true;
            }
        }

        return removed;
    }

private:
    using CacheKey = std::string;
    using Timestamp = std::chrono::steady_clock::time_point;
    // Pair of storage helper and its reference count
    using CacheValue = std::pair<std::shared_ptr<StorageHelper>, Timestamp>;
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

        std::size_t hashValue = std::hash<std::string>{}(key.toStdString());

        return fmt::format("{:016x}-{}", hashValue, type.toStdString());
    }

    std::unique_ptr<StorageHelperCreator<CommunicatorT>> m_creator;
    CacheMap m_cache;
    std::chrono::seconds m_expirySeconds;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_CACHING_STORAGE_HELPER_CREATOR_H