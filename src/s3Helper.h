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
#include "s3HelperParams.h"

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
    explicit S3HelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
    }

    folly::fbstring name() const override { return S3_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"scheme", "hostname", "timeout", "region",
            "validateServerCertificate"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        auto params = S3HelperParams::create(parameters);

        return std::make_shared<KeyValueAdapter>(
            std::make_shared<S3Helper>(params), params, m_executor,
            executionContext);
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
    using params_type = S3HelperParams;

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
    explicit S3Helper(std::shared_ptr<S3HelperParams> params);

    S3Helper(const S3Helper &) = delete;
    S3Helper &operator=(const S3Helper &) = delete;
    S3Helper(S3Helper &&) = delete;
    S3Helper &operator=(S3Helper &&) = delete;

    virtual ~S3Helper() = default;

    folly::fbstring name() const override { return S3_HELPER_NAME; };

    HELPER_PARAM_GETTER(scheme)
    HELPER_PARAM_GETTER(hostname)
    HELPER_PARAM_GETTER(bucketName)
    HELPER_PARAM_GETTER(accessKey)
    HELPER_PARAM_GETTER(secretKey)
    HELPER_PARAM_GETTER(version)
    HELPER_PARAM_GETTER(verifyServerCertificate)
    HELPER_PARAM_GETTER(disableExpectHeader)
    HELPER_PARAM_GETTER(enableClockSkewAdjustment)
    HELPER_PARAM_GETTER(maxConnections)
    HELPER_PARAM_GETTER(fileMode)
    HELPER_PARAM_GETTER(dirMode)
    HELPER_PARAM_GETTER(region)

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

private:
    folly::fbstring getRegion(const folly::fbstring &hostname) const;

    folly::fbstring toEffectiveKey(const folly::fbstring &key) const;
    folly::fbstring fromEffectiveKey(const folly::fbstring &key) const;

    // Prefix relative to the S3 bucket, which should be used on all requests
    // This effectively enables syncing of subdirectories within a bucket
    // or specifying a specific subdirectory of a bucket as space directory
    folly::fbstring m_bucket;
    folly::fbstring m_prefix;
    std::unique_ptr<Aws::S3::S3Client> m_client;
};

/*
 * The S3HelperApiInit class is responsible for initialization and cleanup of
 * AWS SDK C++ library. It should be instantiated prior to any library call.
 */
class S3HelperApiInit {
public:
    S3HelperApiInit() { Aws::InitAPI(m_options); }

    S3HelperApiInit(const S3HelperApiInit &) = delete;
    S3HelperApiInit &operator=(const S3HelperApiInit &) = delete;
    S3HelperApiInit(S3HelperApiInit &&) = delete;
    S3HelperApiInit &operator=(S3HelperApiInit &&) = delete;

    ~S3HelperApiInit() { Aws::ShutdownAPI(m_options); }

private:
    Aws::SDKOptions m_options;
};

} // namespace helpers
} // namespace one

#endif // HELPERS_S3_HELPER_H
