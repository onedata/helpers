/**
 * @file IStorageHelper.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef HELPERS_I_STORAGE_HELPER_H
#define HELPERS_I_STORAGE_HELPER_H


#include <fuse.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <boost/any.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/thread/future.hpp>

#include <unordered_map>
#include <string>
#include <vector>
#include <memory>
#include <system_error>

namespace one
{
namespace helpers
{

struct StorageHelperCTX {

    fuse_file_info &m_ffi;

    StorageHelperCTX(fuse_file_info &ffi)
            : m_ffi(ffi) {
    }

};

typedef StorageHelperCTX& ctx_type;

/**
 * The IStorageHelper interface.
 * Base class of all storage helpers. Unifies their interface.
 * All callback have their equivalent in FUSE API and should be used in that matter.
 */
class IStorageHelper
{
public:
    using ArgsMap = std::unordered_map<std::string, boost::any>;

    virtual ~IStorageHelper() = default;

    virtual boost::shared_future<struct stat> sh_getattr(const boost::filesystem::path &p) = 0;
    virtual boost::shared_future<int> sh_access(const boost::filesystem::path &p, int mask) = 0;
    virtual boost::shared_future<std::string> sh_readlink(const boost::filesystem::path &p) = 0;
    virtual boost::shared_future<std::vector<std::string>>
            sh_readdir(const boost::filesystem::path &p, off_t offset, size_t count, ctx_type ctx) = 0;
    virtual boost::shared_future<int> sh_mknod(const boost::filesystem::path &p, mode_t mode, dev_t rdev) = 0;
    virtual boost::shared_future<int> sh_mkdir(const boost::filesystem::path &p, mode_t mode) = 0;
    virtual boost::shared_future<int> sh_unlink(const boost::filesystem::path &p) = 0;
    virtual boost::shared_future<int> sh_rmdir(const boost::filesystem::path &p) = 0;
    virtual boost::shared_future<int>
            sh_symlink(const boost::filesystem::path &from, const boost::filesystem::path &to) = 0;
    virtual boost::shared_future<int>
            sh_rename(const boost::filesystem::path &from, const boost::filesystem::path &to) = 0;
    virtual boost::shared_future<int>
            sh_link(const boost::filesystem::path &from, const boost::filesystem::path &to) = 0;
    virtual boost::shared_future<int> sh_chmod(const boost::filesystem::path &p, mode_t mode) = 0;
    virtual boost::shared_future<int> sh_chown(const boost::filesystem::path &p, uid_t uid, gid_t gid) = 0;
    virtual boost::shared_future<int> sh_truncate(const boost::filesystem::path &p, off_t size) = 0;


    virtual boost::shared_future<int> sh_open(const boost::filesystem::path &p, ctx_type ctx) = 0;
    virtual boost::shared_future<boost::asio::mutable_buffer>
            sh_read(const boost::filesystem::path &p, boost::asio::mutable_buffer buf, off_t offset,
                    ctx_type ctx) = 0;
    virtual boost::shared_future<int>
            sh_write(const boost::filesystem::path &p, boost::asio::const_buffer buf, off_t offset,
                     ctx_type ctx) = 0;
    virtual boost::shared_future<int> sh_release(const boost::filesystem::path &p, ctx_type ctx) = 0;
    virtual boost::shared_future<int> sh_flush(const boost::filesystem::path &p, ctx_type ctx) = 0;
    virtual boost::shared_future<int>
            sh_fsync(const boost::filesystem::path &p, int isdatasync, ctx_type ctx) = 0;


protected:
    template<class T>
    static void setPosixError(std::shared_ptr<boost::promise<T>> p, int posix_code)
    {
        posix_code = posix_code > 0 ? posix_code : -posix_code;
        p->set_exception(std::system_error(posix_code, std::system_category()));
    }
};

} // namespace helpers
} // namespace one


#endif // HELPERS_I_STORAGE_HELPER_H
