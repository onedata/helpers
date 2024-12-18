/**
 * @file glusterfsHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_GLUSTERFS_HELPER_H
#define HELPERS_GLUSTERFS_HELPER_H

#include "helpers/storageHelper.h"

#include "glusterfsHelperParams.h"

#include <folly/executors/IOExecutor.h>
#include <glusterfs/api/glfs-handles.h>
#include <glusterfs/api/glfs.h>

#include <tuple>

namespace boost {
namespace filesystem {
/**
 * Return a child path suffix which is relative to parent path,
 * for example:
 *
 * makeRelative("/DIR1/DIR2", "DIR1/DIR2/DIR3/file.txt") -> "DIR3/file.txt"
 *
 * @param parent Parent path
 * @param child Child path
 *
 * @return Relative Subpath of the child with respect to parent directory
 */
path makeRelative(path parent, path child);

} // namespace filesystem
} // namespace boost

namespace one {
namespace helpers {

class GlusterFSHelper;

/**
 * Gluster xlator options enable customizing connection to a particular
 * volume on the level of specific GlusterFS translators (a.k.a. plugins)
 *
 * Here the pair represents:
 *  (TRANSLATOR_OPTION_NAME, OPTION_VALUE)
 */
using GlusterFSXlatorOptions =
    folly::fbvector<std::pair<folly::fbstring, folly::fbstring>>;

/**
 * This class holds a GlusterFS connection object which should be maintained
 * between helpers creation and destruction.
 */
struct GlusterFSConnection {
    std::shared_ptr<glfs_t> glfsCtx{nullptr};
    bool connected = false;

    static folly::fbstring generateCtxId(const folly::fbstring &hostname,
        int port, const folly::fbstring &volume)
    {
        return hostname + "::" + folly::fbstring(std::to_string(port)) +
            "::" + volume;
    }
};

/**
 * The @c FileHandle implementation for GlusterFS storage helper.
 */
class GlusterFSFileHandle
    : public FileHandle,
      public std::enable_shared_from_this<GlusterFSFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId Path to the file or directory.
     * @param helper A pointer to the helper that created the handle.
     * @param glfsFd A reference to @c glfs_fd_t struct for GlusterFS direct
     * access to a file descriptor.
     */
    GlusterFSFileHandle(const folly::fbstring &fileId,
        std::shared_ptr<GlusterFSHelper> helper,
        std::shared_ptr<glfs_fd_t> glfsFd, uid_t uid, gid_t gid);

    GlusterFSFileHandle(const GlusterFSFileHandle &) = delete;
    GlusterFSFileHandle &operator=(const GlusterFSFileHandle &) = delete;
    GlusterFSFileHandle(GlusterFSFileHandle &&) = delete;
    GlusterFSFileHandle &operator=(GlusterFSFileHandle &&) = delete;

    ~GlusterFSFileHandle();

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    folly::Future<folly::Unit> release() override;

    folly::Future<folly::Unit> flush() override;

    folly::Future<folly::Unit> fsync(bool isDataSync) override;

    const Timeout &timeout() override;

private:
    std::shared_ptr<glfs_fd_t> m_glfsFd;
    std::atomic_bool m_needsRelease{true};
    const uid_t m_uid;
    const gid_t m_gid;
};

/**
 * The GlusterFSHelper class provides access to Gluster volume
 * directly using libgfapi library.
 */
class GlusterFSHelper : public StorageHelper,
                        public std::enable_shared_from_this<GlusterFSHelper> {
public:
    using params_type = GlusterFSHelperParams;

    /**
     * Constructor.
     * @param mountPoint Root folder within the volume, all operations on the
     *                   volume will be relative to it.
     * @param uid The uid of the user on whose behalf the storage access
     *            operations are performed.
     * @param gid The gid of the user on whose behalf the storage access
     *            operations are performed.
     * @param hostname The GlusterFS volfile server hostname.
     * @param port The GlusterFS volfile server port.
     * @param volume The GlusterFS volume name.
     * @param transport The GlusterFS volfile server transport, possible values
     *                  are: "socket", "tcp", "rdma".
     * @param xlatorOptions Custom xlator options which should be overwritten
     *                      for this connection in the format:
     *                      TRANSLATOR1OPTION1=VALUE1;TRANSLATOR2OPTION2=OPTION2;...
     * @param executor Executor that will drive the helper's async operations.
     * @param timeout Operation timeout.
     */
    GlusterFSHelper(std::shared_ptr<GlusterFSHelperParams> params,
        std::shared_ptr<folly::Executor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    GlusterFSHelper(const GlusterFSHelper &) = delete;
    GlusterFSHelper &operator=(const GlusterFSHelper &) = delete;
    GlusterFSHelper(GlusterFSHelper &&) = delete;
    GlusterFSHelper &operator=(GlusterFSHelper &&) = delete;

    ~GlusterFSHelper() override = default;

    folly::fbstring name() const override { return GLUSTERFS_HELPER_NAME; };

    HELPER_PARAM_GETTER(mountPoint)
    HELPER_PARAM_GETTER(uid)
    HELPER_PARAM_GETTER(gid)
    HELPER_PARAM_GETTER(hostname)
    HELPER_PARAM_GETTER(port)
    HELPER_PARAM_GETTER(volume)
    HELPER_PARAM_GETTER(transport)
    HELPER_PARAM_GETTER(xlatorOptions)

    folly::Future<folly::Unit> checkStorageAvailability() override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count) override;

    folly::Future<folly::fbstring> readlink(
        const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t unmaskedMode, const FlagsSet &flags,
        const dev_t rdev) override;

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> symlink(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> link(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> chown(const folly::fbstring &fileId,
        const uid_t uid, const gid_t gid) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId) override;

    std::shared_ptr<folly::Executor> executor() override { return m_executor; };

    folly::Future<folly::Unit> connect();

    /**
     * Parse custom GlusterFS xlator options for the volume, which
     * the helper should use to connect to.
     *
     * The options should be in the form:
     *  translator1option1=value1;translator2option2=value2;...
     *
     * If the 'options' argument does not conform to the pattern,
     * runtime_error is thrown.
     *
     * @param options The string with encoded xlator options
     * @return Vector of pairs of xlator options
     */
    static GlusterFSXlatorOptions parseXlatorOptions(
        const folly::fbstring &options);

    boost::filesystem::path root(const folly::fbstring &fileId) const;

    boost::filesystem::path relative(const folly::fbstring &fileId) const;

private:
    std::shared_ptr<folly::Executor> m_executor;

    std::shared_ptr<glfs_t> m_glfsCtx;
};

/**
 * An implementation of @c StorageHelperFactory for GlusterFS storage helper.
 */
class GlusterFSHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param executor executor that will be used for some async operations.
     */
    explicit GlusterFSHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
    }

    folly::fbstring name() const override { return GLUSTERFS_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"hostname", "port", "transport", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {

        return std::make_shared<GlusterFSHelper>(
            GlusterFSHelperParams::create(parameters), m_executor,
            executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_GLUSTERFS_HELPER_H
