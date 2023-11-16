/**
 * @file s3Helper.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "s3Helper.h"
#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <boost/algorithm/string.hpp>
#include <folly/Range.h>
#include <glog/stl_logging.h>

#include <algorithm>
#include <functional>

namespace {

const std::map<Aws::S3::S3Errors, std::errc> &ErrorMappings()
{
    const static std::map<Aws::S3::S3Errors, std::errc> g_errors = {
        {Aws::S3::S3Errors::INVALID_PARAMETER_VALUE,
            std::errc::invalid_argument},
        {Aws::S3::S3Errors::MISSING_ACTION, std::errc::not_supported},
        {Aws::S3::S3Errors::SERVICE_UNAVAILABLE, std::errc::host_unreachable},
        {Aws::S3::S3Errors::NETWORK_CONNECTION, std::errc::network_unreachable},
        {Aws::S3::S3Errors::REQUEST_EXPIRED, std::errc::timed_out},
        {Aws::S3::S3Errors::ACCESS_DENIED, std::errc::permission_denied},
        {Aws::S3::S3Errors::UNKNOWN, std::errc::no_such_file_or_directory},
        {Aws::S3::S3Errors::NO_SUCH_BUCKET,
            std::errc::no_such_file_or_directory},
        {Aws::S3::S3Errors::NO_SUCH_KEY, std::errc::no_such_file_or_directory},
        {Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
            std::errc::no_such_file_or_directory}};
    return g_errors;
}

// Retry only in case one of these errors occured
const std::set<Aws::S3::S3Errors> &S3RetryErrors()
{
    static const std::set<Aws::S3::S3Errors> S3_RETRY_ERRORS = {
        Aws::S3::S3Errors::INTERNAL_FAILURE,
        Aws::S3::S3Errors::INVALID_QUERY_PARAMETER,
        Aws::S3::S3Errors::INVALID_PARAMETER_COMBINATION,
        Aws::S3::S3Errors::INVALID_PARAMETER_VALUE,
        Aws::S3::S3Errors::REQUEST_EXPIRED,
        Aws::S3::S3Errors::SERVICE_UNAVAILABLE, Aws::S3::S3Errors::SLOW_DOWN,
        Aws::S3::S3Errors::THROTTLING, Aws::S3::S3Errors::NETWORK_CONNECTION};
    return S3_RETRY_ERRORS;
}

template <typename Outcome>
std::error_code getReturnCode(const Outcome &outcome)
{
    LOG_FCALL();
    if (outcome.IsSuccess())
        return one::helpers::constants::SUCCESS_CODE;

    auto error = std::errc::io_error;
    auto search = ErrorMappings().find(outcome.GetError().GetErrorType());
    if (search != ErrorMappings().end())
        error = search->second;

    return {static_cast<int>(error), std::system_category()};
}

template <typename Outcome>
void throwOnError(const folly::fbstring &operation, const Outcome &outcome)
{
    auto code = getReturnCode(outcome);
    if (!code)
        return;

    auto msg =
        operation.toStdString() + "': " + outcome.GetError().GetMessage();

    LOG_DBG(1) << "Operation " << operation << " failed with message " << msg;

    if (operation == "PutObject") {
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.s3.errors.write");
    }
    else if (operation == "GetObject") {
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.s3.errors.read");
    }

    throw std::system_error{code, std::move(msg)};
}

template <typename T>
bool S3RetryCondition(const T &outcome, const std::string &operation)
{
    auto result = outcome.IsSuccess() ||
        !S3RetryErrors().count(outcome.GetError().GetErrorType());

    if (!result) {
        LOG(WARNING) << "Retrying S3 helper operation '" << operation
                     << "' due to error: "
                     << outcome.GetError().GetMessage().c_str();
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.s3." + operation + ".retries");
    }

    return result;
}

} // namespace

namespace one {
namespace helpers {

namespace {
long to_ms(const Timeout &duration)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration)
        .count();
}
}

S3Helper::S3Helper(const folly::fbstring &hostname,
    const folly::fbstring &bucketName, const folly::fbstring &accessKey,
    const folly::fbstring &secretKey, const bool verifyServerCertificate,
    const bool disableExpectHeader, const bool enableClockSkewAdjustment,
    const int maxConnections, const std::size_t maximumCanonicalObjectSize,
    const mode_t fileMode, const mode_t dirMode, const bool useHttps,
    folly::fbstring region, Timeout timeout, StoragePathType storagePathType)
    : KeyValueHelper{false, storagePathType, maximumCanonicalObjectSize}
    , m_timeout{timeout}
    , m_fileMode{fileMode}
    , m_dirMode{dirMode}
    , m_defaultRegion{std::move(region)}
{
    LOG_FCALL() << LOG_FARG(hostname) << LOG_FARG(m_bucket)
                << LOG_FARG(accessKey) << LOG_FARG(secretKey)
                << LOG_FARG(useHttps) << LOG_FARG(timeout.count());

    static S3HelperApiInit init;

    // Split bucket and prefix
    std::vector<folly::fbstring> pathComponents;
    folly::split('/', bucketName, pathComponents, true);
    if (pathComponents.empty())
        throw std::invalid_argument("Invalid bucket name.");

    if (pathComponents.size() == 1) {
        m_bucket = pathComponents[0];
        m_prefix = "";
    }
    else {
        m_bucket = pathComponents[0];
        folly::join(
            '/', ++pathComponents.begin(), pathComponents.end(), m_prefix);
    }

    Aws::Client::ClientConfiguration configuration;
    configuration.connectTimeoutMs = m_timeout.count();
    configuration.requestTimeoutMs = m_timeout.count();
    configuration.enableClockSkewAdjustment = enableClockSkewAdjustment;
    configuration.verifySSL = verifyServerCertificate;
    configuration.disableExpectHeader = disableExpectHeader;
    configuration.maxConnections = maxConnections;
    configuration.connectTimeoutMs = to_ms(m_timeout);
    configuration.requestTimeoutMs = to_ms(m_timeout);

    configuration.region = getRegion(hostname).c_str();
    configuration.endpointOverride = hostname.c_str();
    if (!useHttps)
        configuration.scheme = Aws::Http::Scheme::HTTP;

    if (accessKey.empty() && secretKey.empty()) {
        m_client = std::make_unique<Aws::S3::S3Client>(configuration,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    }
    else {
        Aws::Auth::AWSCredentials credentials{
            accessKey.c_str(), secretKey.c_str()};
        m_client = std::make_unique<Aws::S3::S3Client>(credentials,
            configuration,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    }
}

folly::fbstring S3Helper::getRegion(const folly::fbstring &hostname)
{
    LOG_FCALL() << LOG_FARG(hostname);

    folly::fbvector<folly::fbstring> regions{"us-east-2", "us-east-1",
        "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-1",
        "ap-northeast-3", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
        "ap-northeast-1", "ca-central-1", "cn-north-1", "cn-northwest-1",
        "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3",
        "eu-north-1", "me-south-1", "sa-east-1"};

    LOG_DBG(1) << "Attempting to determine S3 region based on hostname: "
               << hostname;

    for (const auto &region : regions) {
        if (hostname.find(region) != folly::fbstring::npos) {
            LOG_DBG(1) << "Using S3 region: " << region;
            return region;
        }
    }

    LOG_DBG(1) << "Using default S3 region " << m_defaultRegion;

    return m_defaultRegion;
}

folly::fbstring S3Helper::toEffectiveKey(const folly::fbstring &key) const
{
    if (m_prefix.empty())
        return key;

    if (key[0] == '/')
        return m_prefix + key;

    return m_prefix + "/" + key;
}

folly::fbstring S3Helper::fromEffectiveKey(const folly::fbstring &key) const
{
    if (m_prefix.empty())
        return key;

    std::vector<folly::fbstring> prefixComponents;
    folly::split('/', m_prefix, prefixComponents, true);

    std::vector<folly::fbstring> pathComponents;
    folly::split('/', key, pathComponents, true);

    auto it = pathComponents.begin();
    std::advance(it, prefixComponents.size());

    std::string result;
    folly::join('/', it, pathComponents.end(), result);

    if (key.back() == '/')
        result += "/";

    return result;
}

folly::IOBufQueue S3Helper::getObject(
    const folly::fbstring &key, const off_t offset, const std::size_t size)
{
    using Aws::Client::AsyncCallerContext;
    using Aws::S3::S3Client;
    using Aws::S3::Model::GetObjectOutcome;
    using Aws::S3::Model::GetObjectRequest;
    using Aws::Utils::Threading::Semaphore;
    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(offset) << LOG_FARG(size);

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
    if (size == 0U)
        return buf;

    auto effectiveKey = toEffectiveKey(key);

    log_timer<> logTimer;

    char *data = static_cast<char *>(buf.preallocate(size, size).first);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(m_bucket.c_str());
    request.SetKey(effectiveKey.c_str());
    request.SetRange(
        rangeToString(offset, static_cast<off_t>(offset + size - 1)).c_str());
    request.SetResponseStreamFactory([data = data, size] {
        // NOLINTNEXTLINE
        auto stream = new std::stringstream;
#if !defined(__APPLE__)
        /**
         * pubsetbuf() implementation depends on libstdc++, and on some
         * platforms including OSX it does not work and data must be copied
         */
        stream->rdbuf()->pubsetbuf(data, size);
#else
        (void)data;
        (void)size;
#endif
        return stream;
    });

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.s3.read");

    auto outcome = retry(
        [&, request = std::move(request)]() {
            folly::Promise<GetObjectOutcome> outcomePromise;
            auto outcomeFuture = outcomePromise.getFuture();

            m_client->GetObjectAsync(
                request,
                [&outcomePromise](const S3Client * /*client*/,
                    const GetObjectRequest & /*request*/,
                    GetObjectOutcome getObjectOutcome,
                    const std::shared_ptr<const AsyncCallerContext> & /*ctx*/) {
                    outcomePromise.setValue(std::move(getObjectOutcome));
                },
                nullptr);
            return std::move(outcomeFuture).get();
        },
        std::bind(S3RetryCondition<Aws::S3::Model::GetObjectOutcome>,
            std::placeholders::_1, "GetObject"));

    auto code = getReturnCode(outcome);

    if (code != constants::SUCCESS_CODE) {
        // In case the read is from outside of the valid range, return empty buf
        if (outcome.GetError().GetExceptionName() == "InvalidRange") {
            auto readBytes = outcome.GetResult().GetContentLength();
            LOG_DBG(1) << "Received InvalidRange error when reading object "
                       << effectiveKey << " in range (" << offset << ", "
                       << offset + size
                       << "). Returning buffer of size: " << readBytes;
            buf.postallocate(static_cast<std::size_t>(readBytes));
            return buf;
        }

        LOG_DBG(1) << "Reading from object " << effectiveKey
                   << " failed with error " << outcome.GetError().GetMessage();

        throwOnError("GetObject", outcome);
    }

#if defined(__APPLE__)
    outcome.GetResult().GetBody().rdbuf()->sgetn(
        data, outcome.GetResult().GetContentLength());
#endif

    auto readBytes = outcome.GetResult().GetContentLength();
    buf.postallocate(static_cast<std::size_t>(readBytes));

    log<read_write_perf>(
        key, "S3Helper", "getObject", offset, size, logTimer.stop());

    LOG_DBG(2) << "Read " << readBytes << " bytes from object " << effectiveKey;

    ONE_METRIC_TIMERCTX_STOP(timer, readBytes);

    return buf;
}

std::size_t S3Helper::putObject(
    const folly::fbstring &key, folly::IOBufQueue buf, const std::size_t offset)
{
    using Aws::Client::AsyncCallerContext;
    using Aws::S3::S3Client;
    using Aws::S3::Model::PutObjectOutcome;
    using Aws::S3::Model::PutObjectRequest;
    using Aws::Utils::Threading::Semaphore;
    using one::logging::log_timer;
    using one::logging::csv::log;
    using one::logging::csv::read_write_perf;

    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(buf.chainLength());

    assert(offset == 0); // NOLINT

    auto effectiveKey = toEffectiveKey(key);

    auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();
    if (iobuf->isChained()) {
        iobuf->unshare();
        iobuf->coalesce();
    }
    auto size = iobuf->length();

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.s3.write");

    log_timer<> logTimer;

    auto stream = std::make_shared<std::stringstream>();
#if !defined(__APPLE__)
    stream->rdbuf()->pubsetbuf(
        reinterpret_cast<char *>(iobuf->writableData()), size);
#else
    stream->rdbuf()->sputn(const_cast<const char *>(
                               reinterpret_cast<char *>(iobuf->writableData())),
        size);
#endif
    PutObjectRequest request;
    request.SetBucket(m_bucket.toStdString());
    request.SetKey(effectiveKey.toStdString());
    request.SetContentLength(size);
    request.SetBody(stream);

    LOG_DBG(2) << "Attempting to write object " << effectiveKey << " of size "
               << size;

    auto outcome = retry(
        [&, request = std::move(request)]() {
            folly::Promise<PutObjectOutcome> outcomePromise;
            auto outcomeFuture = outcomePromise.getFuture();

            m_client->PutObjectAsync(
                request,
                [&outcomePromise](const S3Client * /*client*/,
                    const PutObjectRequest & /*request*/,
                    PutObjectOutcome putObjectOutcome,
                    const std::shared_ptr<const AsyncCallerContext> & /*ctx*/) {
                    outcomePromise.setValue(std::move(putObjectOutcome));
                },
                nullptr);

            // NOLINTNEXTLINE
            return std::move(outcomeFuture).get();
        },
        std::bind(S3RetryCondition<PutObjectOutcome>, std::placeholders::_1,
            "PutObject"));

    ONE_METRIC_TIMERCTX_STOP(timer, size);

    throwOnError("PutObject", outcome);

    log<read_write_perf>(
        key, "S3Helper", "putObject", offset, size, logTimer.stop());

    LOG_DBG(2) << "Written " << size << " bytes to object " << effectiveKey;

    return size;
}

std::size_t S3Helper::modifyObject(
    const folly::fbstring &key, folly::IOBufQueue buf, const std::size_t offset)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(buf.chainLength())
                << LOG_FARG(offset);

    struct stat attr = {};
    bool exists{false};

    try {
        attr = getObjectInfo(key);
        exists = true;
    }
    catch (std::system_error &error) {
        LOG_DBG(2) << "File " << key
                   << " doesn't exist on this storage - we'll create it";
    }
    catch (...) {
        LOG(ERROR) << "Object info is not supported on this "
                      "storage - ignoring write request";
        return 0;
    }

    auto bufSize = buf.chainLength();

    if (exists) {
        auto originalObject = getObject(key, 0, attr.st_size);
        auto originalSize = originalObject.chainLength();
        folly::IOBufQueue newObject{folly::IOBufQueue::cacheChainLength()};

        // Objects on S3 storage always start at offset zero, so cases where
        // modify offset is smaller then object offset are not possible
        if (offset < originalSize) {
            auto left = originalObject.split(offset);
            // originalObject: [----------------------------------------]
            // buf           :        [---------]
            if (offset + buf.chainLength() < originalSize) {
                originalObject.trimStart(buf.chainLength());
                newObject.append(std::move(left));
                newObject.append(std::move(buf));
                newObject.append(std::move(originalObject));
            }
            // originalObject: [-------------------------]
            // buf           :                       [------------------]
            else {
                newObject.append(std::move(left));
                newObject.append(std::move(buf));
            }
        }
        else {
            // originalObject: [--------------]
            // buf           :                       [------------------]
            newObject = std::move(originalObject);
            std::vector<char> padding(offset - originalSize, 0);
            newObject.append(padding.data(), padding.size());
            newObject.append(std::move(buf));
        }

        putObject(key, std::move(newObject), 0);
        return bufSize;
    }

    if (offset == 0) {
        putObject(key, std::move(buf), offset);
        return bufSize;
    }

    std::vector<char> padding(offset, 0);
    folly::IOBufQueue newObject{folly::IOBufQueue::cacheChainLength()};
    newObject.append(padding.data(), padding.size());
    newObject.append(std::move(buf));
    putObject(key, std::move(newObject), 0);
    return bufSize;
}

void S3Helper::deleteObject(const folly::fbstring &key)
{
    deleteObjects({key});
}

void S3Helper::deleteObjects(const folly::fbvector<folly::fbstring> &keys)
{
    using Aws::Vector;
    using Aws::Client::AsyncCallerContext;
    using Aws::S3::S3Client;
    using Aws::S3::Model::Delete;
    using Aws::S3::Model::DeleteObjectsOutcome;
    using Aws::S3::Model::DeleteObjectsRequest;
    using Aws::S3::Model::ObjectIdentifier;

    LOG_FCALL() << LOG_FARGV(keys);

    LOG_DBG(2) << "Attempting to delete objects: " << LOG_VEC(keys);

    for (auto offset = 0UL; offset < keys.size();
         offset += MAX_DELETE_OBJECTS) {
        Vector<ObjectIdentifier> keyBatch;
        keyBatch.reserve(MAX_DELETE_OBJECTS);

        const std::size_t batchSize =
            std::min<std::size_t>(keys.size() - offset, MAX_DELETE_OBJECTS);

        for (const auto &key :
            folly::range(keys.begin(), keys.begin() + batchSize)) {
            const auto effectiveKey = toEffectiveKey(key).toStdString();

            ObjectIdentifier oi{};
            oi.SetKey(key.toStdString());
            keyBatch.emplace_back(std::move(oi));
        }

        Delete del;
        del.SetObjects(std::move(keyBatch));

        DeleteObjectsRequest request;
        request.SetBucket(m_bucket.toStdString());
        request.SetDelete(del);

        auto outcome = retry(
            [&, request = std::move(request)]() {
                folly::Promise<DeleteObjectsOutcome> outcomePromise;
                auto outcomeFuture = outcomePromise.getFuture();

                m_client->DeleteObjectsAsync(
                    request,
                    [&outcomePromise](const S3Client * /*client*/,
                        const DeleteObjectsRequest & /*request*/,
                        DeleteObjectsOutcome deleteObjectsOutcome,
                        const std::shared_ptr<const AsyncCallerContext>
                            & /*ctx*/) {
                        outcomePromise.setValue(
                            std::move(deleteObjectsOutcome));
                    },
                    nullptr);

                return std::move(outcomeFuture).get();
            },
            std::bind(S3RetryCondition<DeleteObjectsOutcome>,
                std::placeholders::_1, "DeleteObjects"));

        throwOnError("DeleteObjects", outcome);
    }

    LOG_DBG(2) << "Deleted objects: " << LOG_VEC(keys);
}

void S3Helper::multipartCopy(const folly::fbstring &sourceKey,
    const folly::fbstring &destinationKey, const std::size_t blockSize,
    const std::size_t size)
{

    LOG_FCALL() << LOG_FARG(sourceKey) << LOG_FARG(destinationKey)
                << LOG_FARG(blockSize) << LOG_FARG(size);

    if (blockSize == 0) {
        LOG(ERROR)
            << "Cannot perform multipart copy from storage with blocksize 0";
        throw std::system_error{
            {EINVAL, std::system_category()}, "Invalid request"};
    }

    using Aws::Client::AsyncCallerContext;
    using Aws::S3::S3Client;
    using Aws::S3::Model::CompletedMultipartUpload;
    using Aws::S3::Model::CompletedPart;
    using Aws::S3::Model::CompleteMultipartUploadOutcome;
    using Aws::S3::Model::CompleteMultipartUploadRequest;
    using Aws::S3::Model::CreateMultipartUploadOutcome;
    using Aws::S3::Model::CreateMultipartUploadRequest;
    using Aws::S3::Model::UploadPartCopyOutcome;
    using Aws::S3::Model::UploadPartCopyRequest;

    const auto effectiveDestinationKey = toEffectiveKey(destinationKey);

    CreateMultipartUploadRequest createRequest;
    createRequest.SetBucket(m_bucket.toStdString());
    createRequest.SetKey(effectiveDestinationKey.toStdString());
    auto createOutcome = m_client->CreateMultipartUpload(createRequest);
    auto uploadId = createOutcome.GetResult().GetUploadId();

    throwOnError("CreateMultipartUploadrequest", createOutcome);

    LOG_DBG(3) << "Multipart upload id: " << uploadId;
    LOG_DBG(3) << "Bucket is: " << m_bucket;

    auto partNumber = 1UL;
    CompletedMultipartUpload completedMultipartUpload;

    const std::size_t blockCount = std::floor(size / blockSize) + 1;
    std::size_t blockIt = 0;

    auto effectiveSourceKey = toEffectiveKey(sourceKey);

    for (; blockIt < blockCount; blockIt++) {
        if (blockIt * blockSize >= size)
            continue;

        struct stat keyStat = {};
        folly::fbstring key{
            fmt::format("{}/{}", effectiveSourceKey, MAX_OBJECT_ID - blockIt)};
        UploadPartCopyRequest request;
        request.SetBucket(m_bucket.toStdString());
        request.SetKey(effectiveDestinationKey.toStdString());
        request.SetPartNumber(partNumber);
        request.SetUploadId(uploadId);

        try {
            keyStat = getObjectInfo(key);
            request.SetCopySource(m_bucket.toStdString() + key.toStdString());
            request.SetCopySourceRange(
                fmt::format("bytes={}-{}", 0, keyStat.st_size - 1));
        }
        catch (...) {
            // If the key does not exist on the source storage
            // use the destination file as source
            size_t startRange = blockIt * blockSize;
            size_t endRange = blockIt * blockSize + blockSize - 1;
            endRange = std::min(endRange, size - 1);

            request.SetCopySource(
                m_bucket.toStdString() + effectiveDestinationKey.toStdString());
            request.SetCopySourceRange(
                fmt::format("bytes={}-{}", startRange, endRange));
        }

        folly::Promise<UploadPartCopyOutcome> outcomePromise;
        auto outcomeFuture = outcomePromise.getFuture();

        m_client->UploadPartCopyAsync(
            request,
            [&outcomePromise](const S3Client * /*client*/,
                const UploadPartCopyRequest & /*request*/,
                UploadPartCopyOutcome uploadPartCopyOutcome,
                const std::shared_ptr<const AsyncCallerContext> & /*ctx*/) {
                outcomePromise.setValue(std::move(uploadPartCopyOutcome));
            },
            nullptr);

        auto uploadPartOutcome = std::move(outcomeFuture).get();
        // NOLINTNEXTLINE

        throwOnError("UploadPartCopyAsync", uploadPartOutcome);

        CompletedPart completedPart;
        completedPart.SetPartNumber(partNumber++);
        completedPart.SetETag(
            uploadPartOutcome.GetResult().GetCopyPartResult().GetETag());
        completedMultipartUpload.AddParts(completedPart);
    }

    CompleteMultipartUploadRequest completeRequest;
    completeRequest.SetKey(effectiveDestinationKey.toStdString());
    completeRequest.SetBucket(m_bucket.toStdString());
    completeRequest.SetUploadId(uploadId);
    completeRequest.SetMultipartUpload(completedMultipartUpload);

    auto completeOutcome = m_client->CompleteMultipartUpload(completeRequest);

    throwOnError("CompleteMultipartUpload", completeOutcome);

    LOG_DBG(2) << "Completed multipart copy from " << sourceKey << " to "
               << destinationKey;
}

struct stat S3Helper::getObjectInfo(const folly::fbstring &key)
{
    using Aws::Client::AsyncCallerContext;
    using Aws::S3::S3Client;
    using Aws::S3::Model::ListObjectsOutcome;
    using Aws::S3::Model::ListObjectsRequest;

    LOG_FCALL() << LOG_FARG(key);

    auto effectiveKey = toEffectiveKey(key);

    struct stat attr = {};

    // For the root directory always return the defaults
    if (effectiveKey.empty() || effectiveKey == "/") {
        attr.st_mode = S_IFDIR | m_dirMode;
        attr.st_mtim.tv_sec =
            std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::now())
                .time_since_epoch()
                .count();
        attr.st_mtim.tv_nsec = 0;
        attr.st_ctim = attr.st_mtim;
        attr.st_atim = attr.st_mtim;

        return attr;
    }

    folly::fbstring normalizedKey = effectiveKey;

    if (normalizedKey.front() == '/')
        normalizedKey.erase(normalizedKey.begin());

    if (!normalizedKey.empty() && normalizedKey.back() == '/')
        normalizedKey.pop_back();

    if (normalizedKey == "/")
        normalizedKey = "";

    ListObjectsRequest request;
    request.SetBucket(m_bucket.c_str());
    request.SetPrefix(normalizedKey.c_str());
    request.SetMaxKeys(1);
    request.SetDelimiter("/");

    LOG_DBG(2) << "Attempting to get object info for " << normalizedKey
               << " in bucket " << m_bucket;

    auto outcome = retry(
        [&, request = std::move(request)]() {
            folly::Promise<ListObjectsOutcome> outcomePromise;
            auto outcomeFuture = outcomePromise.getFuture();

            m_client->ListObjectsAsync(
                request,
                [&outcomePromise](const S3Client * /*client*/,
                    const ListObjectsRequest & /*request*/,
                    ListObjectsOutcome listObjectsOutcome,
                    const std::shared_ptr<const AsyncCallerContext> & /*ctx*/) {
                    outcomePromise.setValue(std::move(listObjectsOutcome));
                },
                nullptr);
            return std::move(outcomeFuture).get();
        },
        std::bind(S3RetryCondition<ListObjectsOutcome>, std::placeholders::_1,
            "ListObjects"));

    auto code = getReturnCode(outcome);

    if (code != constants::SUCCESS_CODE) {
        LOG_DBG(2) << "Getting object " << normalizedKey
                   << " info failed with error "
                   << outcome.GetError().GetMessage();
        throwOnError("ListObject", outcome);
    }

    // If a result was received, it can mean that either a file exists at this
    // key in which case the returned key is equal to requested key, or it is a
    // prefix shared by more files, and we should treat it as a directory

    // Add common prefixes as directory entries
    if (!outcome.GetResult().GetCommonPrefixes().empty()) {
        auto commonPrefixList = outcome.GetResult().GetCommonPrefixes();

        LOG_DBG(2) << "Received " << commonPrefixList.size()
                   << " directories for key " << normalizedKey << " -- "
                   << commonPrefixList.cbegin()->GetPrefix().c_str();

        attr.st_mode = S_IFDIR | m_dirMode;
        attr.st_mtim.tv_sec =
            std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::now())
                .time_since_epoch()
                .count();
        attr.st_mtim.tv_nsec = 0;
        attr.st_ctim = attr.st_mtim;
        attr.st_atim = attr.st_mtim;

        LOG_DBG(2) << "Returning stat for directory " << normalizedKey;
    }
    else if (!outcome.GetResult().GetContents().empty()) {
        LOG_DBG(2) << "Received " << outcome.GetResult().GetContents().size()
                   << " objects for key " << normalizedKey;

        auto object = outcome.GetResult().GetContents().cbegin();

        attr.st_size = object->GetSize();
        attr.st_mode = S_IFREG | m_fileMode;
        attr.st_mtim.tv_sec =
            std::chrono::time_point_cast<std::chrono::seconds>(
                object->GetLastModified().UnderlyingTimestamp())
                .time_since_epoch()
                .count();
        attr.st_mtim.tv_nsec = 0;
        attr.st_ctim = attr.st_mtim;
        attr.st_atim = attr.st_mtim;

        LOG_DBG(2) << "Returning stat for file " << normalizedKey << " of size "
                   << attr.st_size << " last modified at "
                   << attr.st_mtim.tv_sec;
    }
    else {
        LOG_DBG(2) << "No objects or common prefixes returned for key "
                   << normalizedKey;

        throw std::system_error{
            {ENOENT, std::system_category()}, "Object not found"};
    }

    return attr;
}

ListObjectsResult S3Helper::listObjects(const folly::fbstring &prefix,
    const folly::fbstring &marker, const off_t /*offset*/, const size_t size)
{
    using Aws::Client::AsyncCallerContext;
    using Aws::S3::S3Client;
    using Aws::S3::Model::ListObjectsOutcome;
    using Aws::S3::Model::ListObjectsRequest;

    LOG_FCALL() << LOG_FARG(prefix) << LOG_FARG(marker) << LOG_FARG(size);

    auto effectivePrefix = toEffectiveKey(prefix);

    folly::fbstring normalizedPrefix = effectivePrefix;

    if (!normalizedPrefix.empty() && normalizedPrefix.back() != '/')
        normalizedPrefix += '/';

    if (normalizedPrefix.front() == '/')
        normalizedPrefix.erase(0, 1);

    folly::fbstring normalizedMarker =
        marker.empty() ? "" : toEffectiveKey(marker);
    if (!normalizedMarker.empty() && normalizedMarker.front() == '/')
        normalizedMarker.erase(0, 1);

    ListObjectsRequest request;
    request.SetBucket(m_bucket.c_str());

    request.SetPrefix(normalizedPrefix.c_str());
    request.SetMaxKeys(size);
    request.SetMarker(normalizedMarker.c_str());

    LOG_DBG(2) << "Attempting to list objects at " << normalizedPrefix
               << " in bucket " << m_bucket << " after " << normalizedMarker;

    auto outcome = retry(
        [&, request = std::move(request)]() {
            folly::Promise<ListObjectsOutcome> outcomePromise;
            auto outcomeFuture = outcomePromise.getFuture();

            m_client->ListObjectsAsync(
                request,
                [&outcomePromise](const S3Client * /*client*/,
                    const ListObjectsRequest & /*request*/,
                    ListObjectsOutcome listObjectsOutcome,
                    const std::shared_ptr<const AsyncCallerContext> & /*ctx*/) {
                    outcomePromise.setValue(std::move(listObjectsOutcome));
                },
                nullptr);
            return std::move(outcomeFuture).get();
        },
        std::bind(S3RetryCondition<ListObjectsOutcome>, std::placeholders::_1,
            "ListObjects"));

    auto code = getReturnCode(outcome);

    if (code != constants::SUCCESS_CODE) {
        LOG_DBG(1) << "Listing objects from prefix " << normalizedPrefix
                   << " failed with error " << outcome.GetError().GetMessage();
        throwOnError("ListObject", outcome);
    }

    ListObjectsResult result;

    LOG_DBG(2) << "Received " << outcome.GetResult().GetContents().size()
               << " object keys";

    // Add regular objects as file entries
    for (const auto &object : outcome.GetResult().GetContents()) {
        if (object.GetKey().empty() || object.GetKey() == "/")
            continue;

        folly::fbstring name = fromEffectiveKey(object.GetKey().c_str());

        if (name == "/")
            continue;

        if (name.front() != '/')
            name = "/" + name;

        struct stat attr {
        };
        if (object.GetKey().back() == '/') {
            attr.st_mode = S_IFDIR;
            attr.st_mode = S_IFDIR | m_dirMode;
            attr.st_size = 0;
            name.pop_back();
            if (name.empty())
                continue;
        }
        else {
            attr.st_mode = S_IFREG;
            attr.st_mode = S_IFREG | m_fileMode;
            attr.st_size = object.GetSize();
        }
        attr.st_mtim.tv_sec =
            std::chrono::time_point_cast<std::chrono::seconds>(
                object.GetLastModified().UnderlyingTimestamp())
                .time_since_epoch()
                .count();
        attr.st_mtim.tv_nsec = 0;
        attr.st_ctim = attr.st_mtim;
        attr.st_atim = attr.st_mtim;

        result.emplace_back(std::move(name), attr);
    }

    return result;
}

ListObjectsResult S3Helper::listAllObjects(const folly::fbstring &prefix)
{
    LOG_FCALL() << LOG_FARG(prefix);

    ListObjectsResult res;

    // S3 listObjects returns maximum of 1000 results per call
    const size_t kMaxSingleListObjectsSize = 1000UL;

    // The iteration is based on the name of the last returned key
    // not numeric offset
    folly::fbstring lastPositionMarker{};
    while (true) {
        auto partialRes = listObjects(
            prefix, lastPositionMarker, 0, kMaxSingleListObjectsSize);

        if (partialRes.empty())
            break;

        lastPositionMarker = std::get<0>(partialRes.back());

        for (auto &key : partialRes) {
            res.emplace_back(std::move(key));
        }
    }

    std::sort(res.begin(), res.end(), [](const auto &l, const auto &r) {
        return std::get<0>(l) > std::get<0>(r);
    });

    LOG_DBG(3) << "Returning " << res.size() << " objects with prefix "
               << prefix;

    return res;
}
} // namespace helpers
} // namespace one
