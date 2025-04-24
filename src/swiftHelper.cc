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

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <folly/FBString.h>
#include <folly/FBVector.h>
#include <folly/Range.h>
#include <glog/stl_logging.h>

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
    LOG_FCALL() << LOG_FARG(outcome.httpStatus);

    auto statusCode = outcome.httpStatus;

    auto error = std::errc::io_error;
    auto search = ErrorMappings().find(statusCode);
    if (search != ErrorMappings().end())
        error = search->second;

    return {static_cast<int>(error), std::system_category()};
}

template <typename Outcome>
void throwOnError(folly::fbstring operation, const Outcome &outcome)
{
    LOG_FCALL() << LOG_FARG(operation) << LOG_FARG(outcome.httpStatus);

    if (outcome.value.has_value())
        return;

    auto code = getReturnCode(outcome);
    auto reason =
        "'" + operation.toStdString() + "': " + outcome.msg.toStdString();

    LOG_DBG(1) << "Operation " << operation << " failed with message "
               << outcome.msg;

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
    auto statusCode = outcome.httpStatus;
    auto ret =
        (outcome.value.has_value() || !SWIFTRetryErrors().count(statusCode));

    if (!ret) {
        LOG(WARNING) << "Retrying SWIFT helper operation '" << operation
                     << "' due to error: " << outcome.msg;
        ONE_METRIC_COUNTER_INC(
            "comp.helpers.mod.swift." + operation + ".retries");
    }

    return ret;
}
} // namespace

SwiftClient::SwiftClient(folly::fbstring keystoneUrl,
    folly::fbstring swiftContainer, folly::fbstring username,
    folly::fbstring password, folly::fbstring projectName,
    folly::fbstring userDomainName, folly::fbstring projectDomainName)
    : m_keystoneUrl(std::move(keystoneUrl))
    , m_swiftContainer(std::move(swiftContainer))
    , m_username(std::move(username))
    , m_password(std::move(password))
    , m_projectName(std::move(projectName))
    , m_userDomainName(std::move(userDomainName))
    , m_projectDomainName(std::move(projectDomainName))
{
}

SwiftResult<bool> SwiftClient::containerExists()
{
    LOG_FCALL() << LOG_FARG(m_swiftContainer);

    auto swiftToken = authenticateIfNeeded();

    // Build the URL: <swiftEndpoint>/<container>
    auto containerUrl = swiftToken.swiftEndpoint + "/" + m_swiftContainer;
    Poco::URI uri(containerUrl.toStdString());
    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
    std::string path = uri.getPathAndQuery();
    if (path.empty())
        path = "/";

    // Construct a HEAD request to check if the container exists.
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_HEAD, path,
        Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("X-Auth-Token", swiftToken.token.toStdString());

    try {
        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        session.receiveResponse(response);

        // If container exists, Swift typically returns HTTP_NO_CONTENT (204) or
        // HTTP_OK (200).
        if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_NO_CONTENT ||
            response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK) {
            return SwiftResult<bool>{true, response.getStatus()};
        }

        // If container does not exist, Swift returns HTTP_NOT_FOUND (404).
        if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
            return SwiftResult<bool>{false, response.getStatus()};
        }

        // For any unexpected status, return the status and reason.
        return SwiftResult<bool>{response.getStatus(), response.getReason()};
    }
    catch (Poco::Exception &ex) {
        return SwiftResult<bool>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.message()};
    }
    catch (std::exception &ex) {
        return SwiftResult<bool>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.what()};
    }
}

/**
 * Put object contents in specified range.
 *
 * For this implementation only offset 0 is supported.
 */
SwiftResult<std::size_t> SwiftClient::putObject(
    const folly::fbstring &key, folly::IOBufQueue buf, const std::size_t offset)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(buf.chainLength())
                << LOG_FARG(offset);

    if (offset != 0) {
        throw std::system_error{
            std::make_error_code(std::errc::function_not_supported)};
    }

    // Ensure we are authenticated.
    auto swiftToken = authenticateIfNeeded();
    auto size = buf.chainLength();

    // Build the URL: <swiftEndpoint>/<container>/<key>
    auto objectUrl =
        swiftToken.swiftEndpoint + "/" + m_swiftContainer + "/" + key;
    Poco::URI uri(objectUrl.toStdString());
    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
    std::string path = uri.getPathAndQuery();
    if (path.empty())
        path = "/";

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, path,
        Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("X-Auth-Token", swiftToken.token.toStdString());

    // For demonstration, assume buf.chainLength() returns the number of
    // bytes
    request.setContentLength(buf.chainLength());

    // Extract the payload from the IOBufQueue.
    // (In a production implementation you would iterate the chain.)
    std::string content;
    buf.appendToString(content);

    try {
        std::ostream &os = session.sendRequest(request);
        os << content;
        Poco::Net::HTTPResponse response;
        session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK &&
            response.getStatus() != Poco::Net::HTTPResponse::HTTP_CREATED &&
            response.getStatus() != Poco::Net::HTTPResponse::HTTP_ACCEPTED) {
            return SwiftResult<std::size_t>{
                response.getStatus(), response.getReason()};
        }

        return SwiftResult<std::size_t>{size, response.getStatus()};
    }
    catch (Poco::Exception &ex) {
        return SwiftResult<std::size_t>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.message()};
    }
    catch (std::exception &ex) {
        return SwiftResult<std::size_t>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.what()};
    }
}

/**
 * Delete storage object by key.
 */
SwiftResult<folly::Unit> SwiftClient::deleteObject(const folly::fbstring &key)
{
    auto swiftToken = authenticateIfNeeded();

    auto objectUrl =
        swiftToken.swiftEndpoint + "/" + m_swiftContainer + "/" + key;
    Poco::URI uri(objectUrl.toStdString());
    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
    std::string path = uri.getPathAndQuery();
    if (path.empty())
        path = "/";

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_DELETE, path,
        Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("X-Auth-Token", swiftToken.token.toStdString());

    try {
        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_NO_CONTENT &&
            response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK) {
            return SwiftResult<folly::Unit>{response.getStatus()};
        }
    }
    catch (Poco::Exception &ex) {
        return SwiftResult<folly::Unit>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
    catch (std::exception &ex) {
        return SwiftResult<folly::Unit>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }

    return SwiftResult<folly::Unit>{folly::Unit{}};
}

/**
 * Delete multiple storage objects by key.
 */
SwiftResult<std::vector<SwiftResult<folly::Unit>>> SwiftClient::deleteObjects(
    const folly::fbvector<folly::fbstring> &keys)
{
    SwiftResult<std::vector<SwiftResult<folly::Unit>>> res{
        std::vector<SwiftResult<folly::Unit>>{},
        Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK};

    for (const auto &key : keys) {
        res.value->push_back(deleteObject(key));
    }

    return res;
}

SwiftResult<folly::IOBufQueue> SwiftClient::getObject(
    const folly::fbstring &key, const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(offset) << LOG_FARG(size);

    // Ensure authentication is valid.
    auto swiftToken = authenticateIfNeeded();

    // Build the object URL: <swiftEndpoint>/<container>/<key>
    auto objectUrl =
        swiftToken.swiftEndpoint + "/" + m_swiftContainer + "/" + key;

    LOG_DBG(1) << "Getting object at: " << objectUrl;

    Poco::URI uri(objectUrl.toStdString());
    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
    std::string path = uri.getPathAndQuery();
    if (path.empty())
        path = "/";

    // Construct the GET request with a Range header.
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, path,
        Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("X-Auth-Token", swiftToken.token.toStdString());
    std::string rangeHeader = "bytes=" + std::to_string(offset) + "-" +
        std::to_string(offset + size - 1);
    request.set("Range", rangeHeader);

    try {
        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        std::istream &rs = session.receiveResponse(response);

        // Expect HTTP 200 OK or 206 Partial Content.
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK &&
            response.getStatus() !=
                Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT) {
            return SwiftResult<folly::IOBufQueue>{response.getStatus()};
        }

        // Read the response body into a string.
        std::ostringstream oss;
        Poco::StreamCopier::copyStream(rs, oss);
        std::string responseData = oss.str();

        // Append the response data into a folly::IOBufQueue and return.
        folly::IOBufQueue bufQueue{folly::IOBufQueue::cacheChainLength()};
        bufQueue.append(
            folly::IOBuf::copyBuffer(responseData.data(), responseData.size()));
        return SwiftResult<folly::IOBufQueue>{
            std::move(bufQueue), response.getStatus()};
    }
    catch (Poco::Exception &ex) {
        return SwiftResult<folly::IOBufQueue>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
    catch (std::exception &ex) {
        return SwiftResult<folly::IOBufQueue>{
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
}

SwiftToken SwiftClient::authenticateIfNeeded()
{
    std::lock_guard<std::mutex> guard{m_swiftTokenMutex};

    auto now = std::chrono::system_clock::now();
    if (!m_swiftToken.token.empty() && (now < m_swiftToken.tokenExpiry)) {
        return m_swiftToken;
    }

    // Build the Keystone authentication JSON payload.
    Poco::JSON::Object identityObj;
    Poco::JSON::Array methods;
    methods.add("password");
    identityObj.set("methods", methods);

    Poco::JSON::Object passwordObj;
    Poco::JSON::Object userObj;
    userObj.set("name", m_username.toStdString());
    Poco::JSON::Object domainObj;
    domainObj.set("name", m_userDomainName.toStdString());
    userObj.set("domain", domainObj);
    userObj.set("password", m_password.toStdString());
    passwordObj.set("user", userObj);
    identityObj.set("password", passwordObj);

    Poco::JSON::Object scopeObj;
    Poco::JSON::Object projectObj;
    projectObj.set("name", m_projectName.toStdString());
    Poco::JSON::Object projDomainObj;
    projDomainObj.set("name", m_projectDomainName.toStdString());
    projectObj.set("domain", projDomainObj);
    scopeObj.set("project", projectObj);

    Poco::JSON::Object authContent;
    authContent.set("identity", identityObj);
    authContent.set("scope", scopeObj);

    Poco::JSON::Object root;
    root.set("auth", authContent);

    std::stringstream ss;
    root.stringify(ss);

    // Compose the Keystone authentication URL.
    auto url = m_keystoneUrl + "/auth/tokens";
    Poco::URI uri(url.toStdString());
    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
    std::string path = uri.getPathAndQuery();
    if (path.empty())
        path = "/";

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, path,
        Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentType("application/json");
    request.setContentLength(ss.str().size());

    LOG_DBG(1) << "Authenticating at " << url << " with " << ss.str();

    try {
        std::ostream &os = session.sendRequest(request);
        os << ss.str();

        Poco::Net::HTTPResponse response;
        std::istream &is = session.receiveResponse(response);
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_CREATED) {
            throw std::runtime_error("Authentication failed. HTTP Status: " +
                std::to_string(response.getStatus()));
        }

        // The token is returned in the header.
        m_swiftToken.token = response.get("X-Subject-Token", "");

        // Parse the JSON response to get token expiration and service
        // catalog.
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var parsedResult = parser.parse(is);
        Poco::JSON::Object::Ptr rootObj =
            parsedResult.extract<Poco::JSON::Object::Ptr>();
        Poco::JSON::Object::Ptr tokenObj = rootObj->getObject("token");
        const auto expiresAt = tokenObj->getValue<std::string>("expires_at");

        // Parse the expiration time (assuming format "YYYY-MM-DDTHH:MM:SS")
        Poco::DateTime dt;
        int tzd{};
        Poco::DateTimeParser::parse("%Y-%m-%dT%H:%M:%S", expiresAt, dt, tzd);
        std::time_t tt = dt.timestamp().epochTime();
        m_swiftToken.tokenExpiry = std::chrono::system_clock::from_time_t(tt);

        // Find the Swift (object-store) endpoint in the service catalog.
        Poco::JSON::Array::Ptr catalog = tokenObj->getArray("catalog");
        m_swiftToken.swiftEndpoint.clear();
        for (size_t i = 0; i < catalog->size(); ++i) {
            Poco::JSON::Object::Ptr service = catalog->getObject(i);
            const auto type = service->getValue<std::string>("type");
            if (type == "object-store") {
                Poco::JSON::Array::Ptr endpoints =
                    service->getArray("endpoints");
                for (size_t j = 0; j < endpoints->size(); ++j) {
                    Poco::JSON::Object::Ptr endpoint = endpoints->getObject(j);
                    const auto iface =
                        endpoint->getValue<std::string>("interface");
                    if (iface == "public") {
                        m_swiftToken.swiftEndpoint =
                            endpoint->getValue<std::string>("url");
                        break;
                    }
                }
            }
            if (!m_swiftToken.swiftEndpoint.empty())
                break;
        }

        if (m_swiftToken.swiftEndpoint.empty()) {
            throw std::runtime_error(
                "Failed to obtain Swift endpoint from service catalog");
        }

        Poco::URI swiftEndpointURI{m_swiftToken.swiftEndpoint.toStdString()};
        if (swiftEndpointURI.getHost() == "0.0.0.0") {
            swiftEndpointURI.setHost(uri.getHost());
            m_swiftToken.swiftEndpoint = swiftEndpointURI.toString();
        }
    }
    catch (Poco::Exception &ex) {
        throw std::runtime_error("Poco exception during authentication: " +
            std::string(ex.displayText()));
    }

    return m_swiftToken;
}

SwiftHelper::SwiftHelper(std::shared_ptr<SwiftHelperParams> params)
    : KeyValueHelper{params, false}
{
    invalidateParams()->setValue(std::move(params));

    m_containerName = containerName();

    m_client = std::make_unique<SwiftClient>(authUrl().toStdString(),
        containerName().toStdString(), username().toStdString(),
        password().toStdString(), projectName().toStdString(),
        userDomainName().toStdString(), projectDomainName().toStdString());
}

void SwiftHelper::checkStorageAvailability()
{
    LOG_FCALL();
    auto containerExistsResponse = m_client->containerExists();
    throwOnError("checkStorageAvailability", containerExistsResponse);

    if (!*containerExistsResponse.value) {
        throw std::system_error{
            {static_cast<int>(std::errc::no_such_file_or_directory),
                std::system_category()},
            std::string{"Container does not exist"}};
    }
}

folly::IOBufQueue SwiftHelper::getObject(
    const folly::fbstring &key, const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(offset) << LOG_FARG(size);

    LOG_DBG(2) << "Attempting to read " << size << " bytes from object " << key
               << " at offset " << offset;

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.swift.read");

    auto getResponse =
        retry([&]() { return m_client->getObject(key, offset, size); },
            std::bind(SWIFTRetryCondition<SwiftResult<folly::IOBufQueue>>,
                std::placeholders::_1, "GetObjectContent"));

    throwOnError("getObject", getResponse);

    ONE_METRIC_TIMERCTX_STOP(timer, getResponse.value->chainLength());

    LOG_DBG(2) << "Read " << size << " bytes from object " << key;

    return std::move(*getResponse.value);
}

std::size_t SwiftHelper::putObject(
    const folly::fbstring &key, folly::IOBufQueue buf, const std::size_t offset)
{
    LOG_FCALL() << LOG_FARG(key) << LOG_FARG(buf.chainLength());

    assert(offset == 0);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.swift.write");

    LOG_DBG(2) << "Attempting to write object " << key << " of size "
               << buf.chainLength();

    auto createResponse = retry(
        [this, key, buf = std::move(buf),
            offset]() mutable -> SwiftResult<size_t> {
            return m_client->putObject(key, std::move(buf), offset);
        },
        std::bind(SWIFTRetryCondition<SwiftResult<size_t>>,
            std::placeholders::_1, "CreateReplaceObject"));

    throwOnError("putObject", createResponse);

    std::size_t writtenBytes = *createResponse.value;

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

    m_client->deleteObjects(keys);

    LOG_DBG(2) << "Attempting to delete objects: " << LOG_VEC(keys);

    for (auto offset = 0UL; offset < keys.size();
         offset += MAX_DELETE_OBJECTS) {
        folly::fbvector<folly::fbstring> keyBatch;

        const std::size_t batchSize =
            std::min<std::size_t>(keys.size() - offset, MAX_DELETE_OBJECTS);

        for (const auto &key :
            folly::range(keys.begin(), keys.begin() + batchSize))
            keyBatch.emplace_back(key.toStdString());

        auto deleteResponse = retry(
            [&]() { return m_client->deleteObjects(keyBatch); },
            std::bind(SWIFTRetryCondition<
                          SwiftResult<std::vector<SwiftResult<folly::Unit>>>>,
                std::placeholders::_1, "DeleteObjects"));

        throwOnError("deleteObjects", deleteResponse);
    }

    LOG_DBG(2) << "Deleted objects: " << LOG_VEC(keys);
}

} // namespace helpers
} // namespace one
