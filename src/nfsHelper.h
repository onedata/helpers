/**
 * @file nfsHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2021 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_NFS_HELPER_H
#define HELPERS_NFS_HELPER_H

#include "helpers/storageHelper.h"
#include "nfsHelperParams.h"

#include <folly/executors/IOExecutor.h>
#include <nfsc/libnfs.h>
#include <tbb/concurrent_queue.h>

#include <tuple>

namespace one {
namespace helpers {

class NFSHelper;

struct NFSConnection {
    struct nfs_context *nfs{nullptr};
    size_t maxReadSize{0};
    size_t maxWriteSize{0};
    bool isConnected{false};
};

/**
 * The @c FileHandle implementation for GlusterFS storage helper.
 */
class NFSFileHandle : public FileHandle,
                      public std::enable_shared_from_this<NFSFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId Path to the file or directory.
     * @param helper A pointer to the helper that created the handle.
     * @param client A reference to @c NFSClient struct for NFS direct
     * access to a file descriptor.
     */
    NFSFileHandle(const folly::fbstring &fileId,
        std::shared_ptr<NFSHelper> helper, struct nfsfh *nfsFh,
        std::shared_ptr<folly::Executor> executor, Timeout timeout);

    ~NFSFileHandle() override = default;

    NFSFileHandle(const NFSFileHandle &) = delete;
    NFSFileHandle(NFSFileHandle &&) = delete;
    NFSFileHandle &operator=(const NFSFileHandle &) = delete;
    NFSFileHandle &operator=(NFSFileHandle &&) = delete;

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    folly::Future<folly::Unit> release() override;

    folly::Future<folly::Unit> flush() override;

    folly::Future<folly::Unit> fsync(bool isDataSync) override;

    const Timeout &timeout() override { return m_timeout; }

    std::shared_ptr<folly::Executor> executor() { return m_executor; };

private:
    std::shared_ptr<folly::Executor> m_executor;
    Timeout m_timeout;

    struct nfsfh *m_nfsFh;
};

/**
 * The NFSHelper class provides access to Gluster volume
 * directly using libgfapi library.
 */
class NFSHelper : public StorageHelper,
                  public std::enable_shared_from_this<NFSHelper> {
public:
    /**
     * Constructor.
     * @param params
     * @param executor Executor that will drive the helper's async operations.
     * @param timeout Operation timeout.
     */
    NFSHelper(std::shared_ptr<NFSHelperParams> params,
        std::shared_ptr<folly::Executor> executor,
        Timeout timeout = constants::ASYNC_OPS_TIMEOUT,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    virtual ~NFSHelper() = default;

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

    folly::Future<NFSConnection *> connect();

    folly::fbstring name() const override { return NFS_HELPER_NAME; };

    const boost::filesystem::path &volume() const { return P()->volume(); }

    int version() const { return P()->version(); }

    uid_t uid() const { return P()->uid(); }

    gid_t gid() const { return P()->gid(); }

    const folly::fbstring &host() const { return P()->host(); }

    bool dircache() const { return P()->dircache(); }

    int tcpSyncnt() const { return P()->tcpSyncnt(); }

    bool autoreconnect() const { return P()->autoreconnect(); }

    size_t readahead() const { return P()->readahead(); }

    void putBackConnection(NFSConnection *conn);

    std::shared_ptr<folly::Executor> executor() override { return m_executor; };

private:
    std::shared_ptr<NFSHelperParams> P() const
    {
        return std::dynamic_pointer_cast<NFSHelperParams>(params().get());
    }

    std::shared_ptr<folly::Executor> m_executor;
    Timeout m_timeout;

    std::vector<std::unique_ptr<NFSConnection>> m_connections{};
    tbb::concurrent_bounded_queue<NFSConnection *> m_idleConnections{};

    std::atomic_bool m_isConnected{false};
    std::once_flag m_connectionFlag;
};

/**
 * An implementation of @c StorageHelperFactory for POSIX storage helper.
 */
class NFSHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async operations.
     */
    explicit NFSHelperFactory(std::shared_ptr<folly::IOExecutor> executor);

    folly::fbstring name() const;

    std::vector<folly::fbstring> overridableParams() const override;
    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override;

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_NFS_HELPER_H
