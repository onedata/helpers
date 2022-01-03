/**
 * @file s3Helper.h
 * @author Krzysztof Trzepla
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_S3_HELPER_H
#define HELPERS_S3_HELPER_H

#include "keyValueAdapter.h"
#include "keyValueHelper.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <folly/executors/IOExecutor.h>

#include <map>
#include <sstream>

namespace Aws {
namespace S3 {
class S3Client;
}
}

namespace one {
namespace helpers {

class S3Helper;

/**
 * An implementation of @c StorageHelperFactory for S3 storage helper.
 */
class S3HelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param executor executor that will be used for some async operations.
     */
    S3HelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{executor}
    {
    }

    virtual folly::fbstring name() const override { return S3_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"scheme", "hostname", "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        // Default value for maximum object size on S3 storages
        // with canonical paths which will support modification of
        // objects in place.
        // Writes to larger objects will be ignored.
        const std::size_t kDefaultMaximumCanonicalObjectSize =
            64ul * 1024 * 1024;

        const auto kDefaultFileMode = "0664";
        const auto kDefaultDirMode = "0775";

        const auto &scheme = getParam(parameters, "scheme", "https");
        const auto &hostname = getParam(parameters, "hostname");
        const auto &bucketName = getParam(parameters, "bucketName");
        const auto &accessKey =
            getParam<std::string>(parameters, "accessKey", "");
        const auto &secretKey =
            getParam<std::string>(parameters, "secretKey", "");
        const auto version = getParam<int>(parameters, "signatureVersion", 4);
        const auto maximumCanonicalObjectSize = getParam<size_t>(parameters,
            "maximumCanonicalObjectSize", kDefaultMaximumCanonicalObjectSize);
        const auto fileMode =
            getParam(parameters, "fileMode", kDefaultFileMode);
        const auto dirMode = getParam(parameters, "dirMode", kDefaultDirMode);
        const auto storagePathType =
            getParam<StoragePathType>(parameters, "storagePathType");
        Timeout timeout{getParam<std::size_t>(
            parameters, "timeout", constants::ASYNC_OPS_TIMEOUT.count())};
        const auto &blockSize =
            getParam<std::size_t>(parameters, "blockSize", DEFAULT_BLOCK_SIZE);

        if (version != 4)
            throw std::invalid_argument(
                "Unsupported S3 signature version: " + std::to_string(version) +
                ". Currently only supported signature "
                "version is '4'.");

        return std::make_shared<KeyValueAdapter>(
            std::make_shared<S3Helper>(hostname, bucketName, accessKey,
                secretKey, maximumCanonicalObjectSize,
                parsePosixPermissions(fileMode), parsePosixPermissions(dirMode),
                scheme == "https", std::move(timeout), storagePathType),
            m_executor, blockSize, executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

/**
 * The S3Helper class provides access to Simple Storage Service (S3) via AWS
 * SDK.
 */
class S3Helper : public KeyValueHelper {
public:
    /**
     * Constructor.
     * @param hostName Hostname of the S3 server.
     * @param bucketName Name of the used S3 bucket.
     * @param accessKey Access key of the S3 user.
     * @param secretKey Secret key of the S3 user.
     * @param useHttps Determines whether to use https or http connection.
     * should be used to sign requests.
     * @param timeout Asynchronous operations timeout.
     */
    S3Helper(const folly::fbstring &hostname, const folly::fbstring &bucketName,
        const folly::fbstring &accessKey, const folly::fbstring &secretKey,
        const std::size_t maximumCanonicalObjectSize, const mode_t fileMode,
        const mode_t dirMode, const bool useHttps = true,
        Timeout timeout = constants::ASYNC_OPS_TIMEOUT,
        StoragePathType storagePathType = StoragePathType::FLAT);

    folly::fbstring name() const override { return S3_HELPER_NAME; };

    bool supportsBatchDelete() const override { return true; }

    folly::IOBufQueue getObject(const folly::fbstring &key, const off_t offset,
        const std::size_t size) override;

    std::size_t putObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    std::size_t modifyObject(const folly::fbstring &key, folly::IOBufQueue buf,
        const std::size_t offset) override;

    void deleteObject(const folly::fbstring &key) override;

    void deleteObjects(const folly::fbvector<folly::fbstring> &keys) override;

    ListObjectsResult listObjects(const folly::fbstring &prefix,
        const folly::fbstring &marker, const off_t offset,
        const size_t size) override;

    ListObjectsResult listAllObjects(const folly::fbstring &prefix);

    void multipartCopy(const folly::fbstring &sourceKey,
        const folly::fbstring &destinationKey, const std::size_t blockSize,
        const std::size_t size) override;

    struct stat getObjectInfo(const folly::fbstring &key) override;

    const Timeout &timeout() override { return m_timeout; }

private:
    static folly::fbstring getRegion(const folly::fbstring &hostname);

    folly::fbstring toEffectiveKey(const folly::fbstring &key) const;
    folly::fbstring fromEffectiveKey(const folly::fbstring &key) const;

    folly::fbstring m_bucket;
    // Prefix relative to the S3 bucket, which should be used on all requests
    // This effectively enables syncing of subdirectories within a bucket
    // or specifying a specific subdirectory of a bucket as space directory
    folly::fbstring m_prefix;
    std::unique_ptr<Aws::S3::S3Client> m_client;
    Timeout m_timeout;

    const mode_t m_fileMode;
    const mode_t m_dirMode;
};

/*
 * The S3HelperApiInit class is responsible for initialization and cleanup of
 * AWS SDK C++ library. It should be instantiated prior to any library call.
 */
class S3HelperApiInit {
public:
    S3HelperApiInit() { Aws::InitAPI(m_options); }

    ~S3HelperApiInit() { Aws::ShutdownAPI(m_options); }

private:
    Aws::SDKOptions m_options;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_S3_HELPER_H
