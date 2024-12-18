/**
 * @file swiftHelper.cc
 * @author Michal Wrona
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "swiftHelper.h"
#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <folly/FBString.h>
#include <folly/FBVector.h>
#include <folly/Range.h>
#include <glog/stl_logging.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <functional>

namespace std {
template <> struct hash<Poco::Net::HTTPResponse::HTTPStatus> {
    size_t operator()(const Poco::Net::HTTPResponse::HTTPStatus &p) const
    {
        return std::hash<int>()(static_cast<int>(p));
    }
};
} // namespace std

namespace one {
namespace helpers {

namespace {

const std::unordered_map<Poco::Net::HTTPResponse::HTTPStatus, std::errc> &
ErrorMappings()
{
    static const std::unordered_map<Poco::Net::HTTPResponse::HTTPStatus,
        std::errc>
        errors = {
            {Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND,
                std::errc::no_such_file_or_directory},
            {Poco::Net::HTTPResponse::HTTPStatus::
                    HTTP_REQUESTED_RANGE_NOT_SATISFIABLE,
                std::errc::no_such_file_or_directory},
            {Poco::Net::HTTPResponse::HTTPStatus::HTTP_REQUEST_TIMEOUT,
                std::errc::timed_out},
            {Poco::Net::HTTPResponse::HTTPStatus::HTTP_LENGTH_REQUIRED,
                std::errc::invalid_argument},
            {Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED,
                std::errc::permission_denied},
        };
    return errors;
}

// Retry only in case one of these errors occured
const std::set<Poco::Net::HTTPResponse::HTTPStatus> &SWIFTRetryErrors()
{
    static const std::set<Poco::Net::HTTPResponse::HTTPStatus>
        SWIFT_RETRY_ERRORS = {
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_REQUEST_TIMEOUT,
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_GONE,
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_INTERNAL_SERVER_ERROR,
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_BAD_GATEWAY,
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_SERVICE_UNAVAILABLE,
            Poco::Net::HTTPResponse::HTTPStatus::HTTP_GATEWAY_TIMEOUT};
    return SWIFT_RETRY_ERRORS;
}

template <typename Outcome>
std::error_code getReturnCode(const Outcome &outcome)
{
    LOG_FCALL() << LOG_FARG(outcome->getResponse()->getStatus());

    auto statusCode = outcome->getResponse()->getStatus();

    auto error = std::errc::io_error;
    auto search = ErrorMappings().find(statusCode);
    if (search != ErrorMappings().end())
        error = search->second;

    return {static_cast<int>(error), std::system_category()};
}

template <typename Outcome>
void throwOnError(folly::fbstring operation, const Outcome &outcome)
{
    LOG_FCALL() << LOG_FARG(operation)
                << LOG_FARG(outcome->getResponse()->getStatus());

    if (outcome->getError().code == Swift::SwiftError::SWIFT_OK)
        return;

    auto code = getReturnCode(outcome);
    auto reason =
        "'" + operation.toStdString() + "': " + outcome->getError().msg;

    LOG_DBG(1) << "Operation " << operation << " failed with message "
               << outcome->getError().msg;

    if (operation == "putObject") {
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.swift.errors.write");
    }
    else if (operation == "getObject") {
        ONE_METRIC_COUNTER_INC("comp.helpers.mod.swift.errors.read");
    }

    throw std::system_error{code, std::move(reason)};
}

template <typename Outcome>
bool SWIFTRetryCondition(const Outcome &outcome, const std::string &operation)
{
    auto statusCode = outcome->getResponse()->getStatus();
    auto ret = (statusCode == Swift::SwiftError::SWIFT_OK ||
        !SWIFTRetryErrors().count(statusCode));

    if (!ret) {
        LOG(WARNING) << "Retrying SWIFT helper operation '" << operation
                     << "' due to error: " << outcome->getError().msg;
        ONE_METRIC_COUNTER_INC(
            "comp.helpers.mod.swift." + operation + ".retries");
    }

    return ret;
}
} // namespace

SwiftHelper::SwiftHelper(std::shared_ptr<SwiftHelperParams> params)
    : KeyValueHelper{params, false}
{
    invalidateParams()->setValue(std::move(params));

    m_auth = std::make_unique<Authentication>(
        authUrl(), tenantName(), username(), password());
    m_containerName = containerName();
}

void SwiftHelper::checkStorageAvailability()
{
    LOG_FCALL();

    m_auth->getAccount();
}

folly::IOBufQueue SwiftHelper::getObject(
    const folly::fbstring &key, const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(offset) << LOG_FARG(size);

    auto &account = m_auth->getAccount();

    Swift::Container container(&account, m_containerName.toStdString());
    Swift::Object object(&container, key.toStdString());

    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};

    LOG_DBG(2) << "Attempting to read " << size << " bytes from object " << key
               << " at offset " << offset;

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.swift.read");

    auto headers = std::vector<Swift::HTTPHeader>({Swift::HTTPHeader("Range",
        rangeToString(offset, static_cast<off_t>(offset + size - 1)))});

    using GetResponsePtr = std::unique_ptr<Swift::SwiftResult<std::istream *>>;

    auto getResponse = retry(
        [&]() {
            return GetResponsePtr{
                object.swiftGetObjectContent(nullptr, &headers)};
        },
        std::bind(SWIFTRetryCondition<GetResponsePtr>, std::placeholders::_1,
            "GetObjectContent"));

    throwOnError("getObject", getResponse);

    char *data = static_cast<char *>(buf.preallocate(size, size).first);

    auto *const newTail =
        std::copy(std::istreambuf_iterator<char>{*getResponse->getPayload()},
            std::istreambuf_iterator<char>{}, data);

    buf.postallocate(newTail - data);

    ONE_METRIC_TIMERCTX_STOP(
        timer, getResponse->getResponse()->getContentLength());

    LOG_DBG(2) << "Read " << size << " bytes from object " << key;

    return buf;
}

std::size_t SwiftHelper::putObject(
    const folly::fbstring &key, folly::IOBufQueue buf, const std::size_t offset)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(buf.chainLength());

    assert(offset == 0);

    std::size_t writtenBytes = 0;
    auto &account = m_auth->getAccount();

    Swift::Container container(&account, m_containerName.toStdString());
    Swift::Object object(&container, key.toStdString());

    auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.swift.write");

    if (iobuf->isChained()) {
        iobuf->unshare();
        iobuf->coalesce();
    }

    LOG_DBG(2) << "Attempting to write object " << key << " of size "
               << iobuf->length();

    using CreateResponsePtr = std::unique_ptr<Swift::SwiftResult<int *>>;

    auto createResponse = retry(
        [&]() {
            return CreateResponsePtr{object.swiftCreateReplaceObject(
                reinterpret_cast<const char *>(iobuf->data()), iobuf->length(),
                true)};
        },
        std::bind(SWIFTRetryCondition<CreateResponsePtr>, std::placeholders::_1,
            "CreateReplaceObject"));

    throwOnError("putObject", createResponse);

    writtenBytes = iobuf->length();

    ONE_METRIC_TIMERCTX_STOP(timer, writtenBytes);

    LOG_DBG(2) << "Written " << writtenBytes << " bytes to object " << key;

    return writtenBytes;
}

void SwiftHelper::deleteObject(const folly::fbstring &key)
{
    deleteObjects({key});
}

void SwiftHelper::deleteObjects(const folly::fbvector<folly::fbstring> &keys)
{
    LOG_FCALL() << LOG_FARGV(keys);

    auto &account = m_auth->getAccount();

    LOG_DBG(2) << "Attempting to delete objects: " << LOG_VEC(keys);

    Swift::Container container(&account, m_containerName.toStdString());
    for (auto offset = 0UL; offset < keys.size();
         offset += MAX_DELETE_OBJECTS) {
        std::vector<std::string> keyBatch;

        const std::size_t batchSize =
            std::min<std::size_t>(keys.size() - offset, MAX_DELETE_OBJECTS);

        for (const auto &key :
            folly::range(keys.begin(), keys.begin() + batchSize))
            keyBatch.emplace_back(key.toStdString());

        using DeleteResponsePtr =
            std::unique_ptr<Swift::SwiftResult<std::istream *>>;

        auto deleteResponse = retry(
            [&]() {
                return DeleteResponsePtr{
                    container.swiftDeleteObjects(keyBatch)};
            },
            std::bind(SWIFTRetryCondition<DeleteResponsePtr>,
                std::placeholders::_1, "DeleteObjects"));

        throwOnError("deleteObjects", deleteResponse);
    }

    LOG_DBG(2) << "Deleted objects: " << LOG_VEC(keys);
}

SwiftHelper::Authentication::Authentication(const folly::fbstring &authUrl,
    const folly::fbstring &tenantName, const folly::fbstring &userName,
    const folly::fbstring &password)
{
    LOG_FCALL() << LOG_FARG(authUrl) << LOG_FARG(tenantName)
                << LOG_FARG(userName) << LOG_FARG(password);

    m_authInfo.username = userName.toStdString();
    m_authInfo.password = password.toStdString();
    m_authInfo.authUrl = authUrl.toStdString();
    m_authInfo.tenantName = tenantName.toStdString();
    m_authInfo.method = Swift::AuthenticationMethod::KEYSTONE;
}

Swift::Account &SwiftHelper::Authentication::getAccount()
{
    LOG_FCALL();

    std::lock_guard<std::mutex> guard{m_authMutex};
    if (m_account)
        return *m_account;

    auto authResponse = std::unique_ptr<Swift::SwiftResult<Swift::Account *>>(
        Swift::Account::authenticate(m_authInfo, true));
    throwOnError("authenticate", authResponse);

    m_account = std::unique_ptr<Swift::Account>(authResponse->getPayload());
    authResponse->setPayload(nullptr);

    return *m_account;
}

} // namespace helpers
} // namespace one
