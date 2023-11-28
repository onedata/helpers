/**
 * @file xrootdHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"
#include "xrootdHelperParams.h"

#include "helpers/logging.h"

#include <XrdCl/XrdClFile.hh>
#include <folly/Executor.h>
#include <folly/executors/IOExecutor.h>

namespace one {
namespace helpers {

constexpr auto kXRootDRetryCount = 6;

class XRootDHelper;

/**
 * The @c FileHandle implementation for XRootD storage helper.
 */
class XRootDFileHandle : public FileHandle,
                         public std::enable_shared_from_this<XRootDFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId XRootD-specific ID associated with the file.
     * @param helper A pointer to the helper that created the handle.
     */
    XRootDFileHandle(folly::fbstring fileId,
        std::unique_ptr<XrdCl::File> &&file,
        std::shared_ptr<XRootDHelper> helper);

    folly::Future<folly::IOBufQueue> read(
        off_t offset, std::size_t size) override;

    folly::Future<folly::IOBufQueue> read(
        off_t offset, std::size_t size, const int retryCount);

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    folly::Future<std::size_t> write(
        off_t offset, folly::IOBufQueue buf, int retryCount);

    folly::Future<folly::Unit> release() override;

    folly::Future<folly::Unit> release(int retryCount);

    folly::Future<folly::Unit> fsync(bool isDataSync) override;

    folly::Future<folly::Unit> fsync(bool isDataSync, int retryCount);

    const Timeout &timeout() override;

private:
    const folly::fbstring m_fileId;

    std::unique_ptr<XrdCl::File> m_file;
};

/**
 * The XRootDHelper class provides access to XRootD storage via librados
 * library.
 */
class XRootDHelper : public StorageHelper,
                     public std::enable_shared_from_this<XRootDHelper> {
public:
    /**
     * Constructor.
     * operations.
     */
    XRootDHelper(std::shared_ptr<XRootDHelperParams> params,
        std::shared_ptr<folly::IOExecutor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    XRootDHelper(const XRootDHelper &) = delete;
    XRootDHelper &operator=(const XRootDHelper &) = delete;
    XRootDHelper(XRootDHelper &&) = delete;
    XRootDHelper &operator=(XRootDHelper &&) = delete;

    /**
     * Destructor.
     * Closes connection to XRootD storage cluster and destroys internal
     * context object.
     */
    ~XRootDHelper() = default;

    folly::fbstring name() const override { return XROOTD_HELPER_NAME; };

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, int mask) override;

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, int mask, const int retryCount);

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<struct stat> getattr(
        const folly::fbstring &fileId, int retryCount);

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        int /*flags*/, const Params & /*openParams*/) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> unlink(const folly::fbstring &fileId,
        const size_t currentSize, const int retryCount);

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> rmdir(
        const folly::fbstring &fileId, int retryCount);

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize, int retryCount);

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev,
        const int retryCount);

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, mode_t mode) override;

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, mode_t mode, int retryCount);

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to, int retryCount);

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> chown(
        const folly::fbstring &fileId, uid_t uid, gid_t gid) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count) override;

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count,
        const int retryCount);

    std::shared_ptr<folly::Executor> executor() override { return m_executor; }

    XRootDCredentialsType credentialsType() const
    {
        return P()->credentialsType();
    }

    XrdCl::URL url() const { return P()->url(); }

    folly::fbstring credentials() const { return P()->credentials(); }

    mode_t fileModeMask() const { return P()->fileModeMask(); }

    mode_t dirModeMask() const { return P()->dirModeMask(); }

private:
    std::shared_ptr<XRootDHelperParams> P() const
    {
        return std::dynamic_pointer_cast<XRootDHelperParams>(params().get());
    }

    std::shared_ptr<folly::IOExecutor> m_executor;

    XrdCl::FileSystem m_fs;
};

/**
 * An implementation of @c StorageHelperFactory for XRootD storage helper.
 */
class XRootDHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    explicit XRootDHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
        LOG_FCALL();
    }

    folly::fbstring name() const override { return XROOTD_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"url", "timeout", "credentialsType", "credentials"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        return std::make_shared<XRootDHelper>(
            XRootDHelperParams::create(parameters), m_executor,
            executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one
