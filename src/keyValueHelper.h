/**
 * @file keyValueHelper.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_KEY_VALUE_HELPER_H
#define HELPERS_KEY_VALUE_HELPER_H

#include "helpers/storageHelper.h"

#include <iomanip>
#include <vector>

namespace one {
namespace helpers {

constexpr auto RANGE_DELIMITER = "-";
constexpr auto OBJECT_DELIMITER = "/";
constexpr std::size_t MAX_DELETE_OBJECTS = 1000;
constexpr auto MAX_LIST_OBJECTS = 1000;
constexpr auto MAX_OBJECT_ID = 999999;
constexpr auto MAX_OBJECT_ID_DIGITS = 6;

/**
 * The @c KeyValueHelper class provides an interface for all helpers that
 * operates on key-value storage.
 */
class KeyValueHelper {
public:
    KeyValueHelper(const bool randomAccess = false)
        : m_randomAccess{randomAccess}
    {
    }

    virtual ~KeyValueHelper() = default;

    virtual folly::fbstring name() const = 0;

    /**
     * @param prefix Arbitrary sequence of characters that provides value
     * namespace.
     * @param objectId ID associated with the value.
     * @return Key identifying value on the storage.
     */
    virtual folly::fbstring getKey(
        const folly::fbstring &prefix, const uint64_t objectId)
    {
        LOG_FCALL() << LOG_FARG(prefix) << LOG_FARG(objectId);

        std::stringstream ss;
        ss << adjustPrefix(prefix) << std::setfill('0')
           << std::setw(MAX_OBJECT_ID_DIGITS) << MAX_OBJECT_ID - objectId;
        return ss.str();
    }

    /**
     * @param key Sequence of characters identifying value on the storage.
     * @return ObjectId ID associated with the value.
     */
    virtual uint64_t getObjectId(const folly::fbstring &key)
    {
        LOG_FCALL() << LOG_FARG(key);
        const auto pos = key.find_last_of(OBJECT_DELIMITER);
        return MAX_OBJECT_ID - std::stoull(key.substr(pos + 1).toStdString());
    }

    /**
     * @param key Sequence of characters identifying value on the storage.
     * @param buf Buffer used to store returned value.
     * @param offset Distance from the beginning of the value to the first byte
     * returned.
     * @return Value associated with the key.
     */
    virtual folly::IOBufQueue getObject(const folly::fbstring &key,
        const off_t offset, const std::size_t size) = 0;

    /**
     * @param key Sequence of characters identifying value on the storage.
     * @param buf Buffer containing bytes of an object to be stored.
     * @param offset Write the object contents starting at offset. Only
     *               applicable to object storages which allow writing to
     *               objects at specific offsets.
     * @return Number of bytes that has been successfully saved on the storage.
     */
    virtual std::size_t putObject(const folly::fbstring &key,
        folly::IOBufQueue buf, const std::size_t offset) = 0;

    /**
     * @param key Sequence of characters identifying value on the storage.
     * @param buf Buffer containing bytes of an object to be stored.
     * @return Number of bytes that has been successfully saved on the storage.
     */
    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf)
    {
        return putObject(key, std::move(buf), 0);
    }

    /**
     * Modifies a range of bytes from offset to offset+buf.chainLength() with
     * the contents of buf buffer. It does not perform range validation, i.e.
     * the if the buffer extends beyond the size of existing object, the object
     * will be enlarged.
     *
     * On storages which do not support range write, the original object will be
     * downloaded, modified in-memory and uploaded back.
     *
     * @param key Sequence of characters identifying value on the storage.
     * @param buf Buffer containing bytes of an object to be stored.
     * @return Number of bytes that has been successfully saved on the storage.
     */
    virtual std::size_t modifyObject(const folly::fbstring &key,
        folly::IOBufQueue buf, const std::size_t offset)
    {
        throw std::system_error{
            std::make_error_code(std::errc::function_not_supported)};
    };

    /**
     * @param keys Vector of keys of objects to be deleted.
     */
    virtual void deleteObjects(
        const folly::fbvector<folly::fbstring> &keys) = 0;

    virtual folly::fbvector<folly::fbstring> listObjects(
        const folly::fbstring &key, const off_t offset, const size_t size)
    {
        return {};
    }

    virtual struct stat getObjectInfo(const folly::fbstring &key) { return {}; }

    virtual const std::vector<folly::fbstring> getOverridableParams() const
    {
        return {};
    };

    virtual const Timeout &timeout() = 0;

    bool hasRandomAccess() const { return m_randomAccess; }

protected:
    std::string adjustPrefix(const folly::fbstring &prefix) const
    {
        LOG_FCALL() << LOG_FARG(prefix);

        return prefix.substr(prefix.find_first_not_of(OBJECT_DELIMITER))
                   .toStdString() +
            OBJECT_DELIMITER;
    }

    std::string rangeToString(const off_t lower, const off_t upper) const
    {
        LOG_FCALL() << LOG_FARG(lower) << LOG_FARG(upper);

        return "bytes=" + std::to_string(lower) + RANGE_DELIMITER +
            std::to_string(upper);
    }

private:
    const bool m_randomAccess;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_KEY_VALUE_HELPER_H
