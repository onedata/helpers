/**
 * @file webDAVHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"

#include "asioExecutor.h"
#include "logging.h"

#include <folly/Executor.h>
#include <folly/ThreadLocal.h>
#include <folly/executors/IOExecutor.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <proxygen/lib/utils/Base64.h>
#include <proxygen/lib/utils/URL.h>

#include <Poco/Base64Encoder.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Node.h>
#include <Poco/DOM/Text.h>
#include <Poco/URI.h>
#include <Poco/XML/XMLWriter.h>

namespace pxml = Poco::XML;

namespace one {
namespace helpers {

class WebDAVHelper;

enum class WebDAVCredentialsType { NONE, BASIC, TOKEN };

enum class WebDAVRangeWriteSupport {
    NONE,                   // No write support
    SABREDAV_PARTIALUPDATE, // Range write using SabreDAV PATCH extension
    MODDAV_PUTRANGE         // Range write using mod_dav PUT with Content-Range
};
constexpr auto kWebDAVHTTPVersionMajor = 1;
constexpr auto kWebDAVHTTPVersionMinor = 1;

constexpr auto kNSDAV = "DAV:";
constexpr auto kNSOnedata = "http://onedata.org/metadata";

/**
 * HTTP Status Codes
 */
enum HTTPStatus {
    Continue = 100,
    SwitchingProtocols = 101,
    Processing = 102,
    OK = 200,
    Created = 201,
    Accepted = 202,
    NonAuthoritativeInformation = 203,
    NoContent = 204,
    ResetContent = 205,
    PartialContent = 206,
    MultiStatus = 207,
    IMUsed = 226,
    MultipleChoices = 300,
    MovedPermanently = 301,
    Found = 302,
    SeeOther = 303,
    NotModified = 304,
    UseProxy = 305,
    TemporaryRedirect = 307,
    PermanentRedirect = 308,
    BadRequest = 400,
    Unauthorized = 401,
    PaymentRequired = 402,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    NotAcceptable = 406,
    ProxyAuthenticationRequired = 407,
    RequestTimeout = 408,
    Conflict = 409,
    Gone = 410,
    LengthRequired = 411,
    PreconditionFailed = 412,
    PayloadTooLarge = 413,
    URITooLong = 414,
    UnsupportedMediaType = 415,
    RangeNotSatisfiable = 416,
    ExpectationFailed = 417,
    ImATeapot = 418,
    UnprocessableEntity = 422,
    Locked = 423,
    FailedDependency = 424,
    UpgradeRequired = 426,
    PreconditionRequired = 428,
    TooManyRequests = 429,
    RequestHeaderFieldsTooLarge = 431,
    UnavailableForLegalReasons = 451,
    InternalServerError = 500,
    NotImplemented = 501,
    BadGateway = 502,
    ServiceUnavailable = 503,
    GatewayTimeout = 504,
    HTTPVersionNotSupported = 505,
    VariantAlsoNegotiates = 506,
    InsufficientStorage = 507,
    NetworkAuthenticationRequired = 511
};

/**
 * The WebDAVHelper class provides access to WebDAV storage via librados
 * library.
 */
class WebDAVHelper : public StorageHelper,
                     public proxygen::HTTPConnector::Callback,
                     public std::enable_shared_from_this<WebDAVHelper> {
public:
    /**
     * Constructor.
     * @param endpoint Complete WebDAV endpoint.
     * @param credentialsType Type of credentials to use.
     * @param credentials Actual credentials, e.g. basic auth pair or access
     * token
     * @param authorizationHeader Optional authorization header to use with
     * access token
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    WebDAVHelper(Poco::URI endpoint, bool verifyServerCertificate,
        WebDAVCredentialsType credentialsType, folly::fbstring credentials,
        folly::fbstring authorizationHeader,
        WebDAVRangeWriteSupport rangeWriteSupport,
        std::shared_ptr<folly::IOExecutor> executor,
        Timeout timeout = ASYNC_OPS_TIMEOUT);

    /**
     * Destructor.
     * Closes connection to WebDAV storage cluster and destroys internal
     * context object.
     */
    ~WebDAVHelper();

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<FileHandlePtr> open(
        const folly::fbstring &fileId, const int, const Params &) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override;

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count) override;

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId) override;

    const Timeout &timeout() override { return m_timeout; }

    /**
     * Establishes connection to the WebDAV storage cluster.
     */
    folly::Future<folly::Unit> connect();

    proxygen::HTTPUpstreamSession *session() const
    {
        return m_context->session;
    }
    proxygen::HTTPConnector *connector() const
    {
        return m_context->connector.get();
    }
    folly::HHWheelTimer *timer() { return m_context->timer.get(); }

    std::shared_ptr<folly::IOExecutor> executor() { return m_executor; }

    WebDAVRangeWriteSupport rangeWriteSupport() const
    {
        return m_rangeWriteSupport;
    }

    WebDAVCredentialsType credentialsType() const { return m_credentialsType; }

    Poco::URI endpoint() const { return m_endpoint; }

    folly::fbstring credentials() const { return m_credentials; }

    folly::fbstring authorizationHeader() const
    {
        return m_authorizationHeader;
    }

    /**
     * \defgroup proxygen::HTTPConnector methods
     * @{
     */
    void connectSuccess(proxygen::HTTPUpstreamSession *session) override;
    void connectError(const folly::AsyncSocketException &ex) override;
    /**@{*/

private:
    struct WebDAVContext {
        std::unique_ptr<proxygen::HTTPConnector> connector;
        proxygen::HTTPUpstreamSession *session{nullptr};
        folly::HHWheelTimer::UniquePtr timer;

        folly::Promise<folly::Unit> connectionPromise;
    };

    Poco::URI m_endpoint;
    bool m_verifyServerCertificate;
    WebDAVCredentialsType m_credentialsType;
    folly::fbstring m_credentials;
    folly::fbstring m_authorizationHeader;
    WebDAVRangeWriteSupport m_rangeWriteSupport;

    std::shared_ptr<folly::IOExecutor> m_executor;
    Timeout m_timeout;

    folly::ThreadLocal<WebDAVContext> m_context;

    pxml::NamespaceSupport m_nsMap;
};

template <class T> using PAPtr = Poco::AutoPtr<T>;

/**
 * Base class for creating WebDAV request/response handlers
 */
class WebDAVRequest : public proxygen::HTTPTransactionHandler {
public:
    WebDAVRequest(const WebDAVHelper &helper);

    virtual ~WebDAVRequest() = default;

    /**
     * \defgroup proxygen::HTTPTransactionHandler methods
     * @{
     */
    virtual void setTransaction(
        proxygen::HTTPTransaction *txn) noexcept override;
    virtual void detachTransaction() noexcept override;
    virtual void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;
    virtual void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override;
    virtual void onTrailers(
        std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept override;
    virtual void onEOM() noexcept override;
    virtual void onUpgrade(
        proxygen::UpgradeProtocol protocol) noexcept override;
    virtual void onError(
        const proxygen::HTTPException &error) noexcept override = 0;
    virtual void onEgressPaused() noexcept override;
    virtual void onEgressResumed() noexcept override;
    /**@{*/

    proxygen::HTTPMessage request;

protected:
    proxygen::HTTPUpstreamSession *m_session;
    proxygen::HTTPTransaction *m_txn;

    uint16_t m_resultCode;
    std::unique_ptr<folly::IOBufQueue> m_resultBody;

    // Make sure the request instance is not destroyed before Proxygen
    // transaction is detached
    std::shared_ptr<WebDAVRequest> m_destructionGuard;
};

/**
 * GET request/response handler
 */
class WebDAVGET : public WebDAVRequest,
                  public std::enable_shared_from_this<WebDAVGET> {
public:
    WebDAVGET(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::IOBufQueue> operator()(
        const folly::fbstring &resource, const off_t offset, const size_t size);

    void onEOM() noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::IOBufQueue> m_resultPromise;

    // Some HTTP server implementations do not handle "Range: bytes=0-0"
    // request properly, in which case we have to download the first 2
    // bytes i.e. "Range: bytes=0-1", and then return the first byte
    bool m_firstByteRequest{false};
};

/**
 * PUT request/response handler
 */
class WebDAVPUT : public WebDAVRequest,
                  public std::enable_shared_from_this<WebDAVPUT> {
public:
    WebDAVPUT(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(const folly::fbstring &resource,
        const off_t offset, std::unique_ptr<folly::IOBuf> buf);

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * PATCH request/response handler
 */
class WebDAVPATCH : public WebDAVRequest,
                    public std::enable_shared_from_this<WebDAVPATCH> {
public:
    WebDAVPATCH(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(const folly::fbstring &resource,
        const off_t offset, std::unique_ptr<folly::IOBuf> buf);

    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * MKCOL request/response handler
 */
class WebDAVMKCOL : public WebDAVRequest,
                    public std::enable_shared_from_this<WebDAVMKCOL> {
public:
    WebDAVMKCOL(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(const folly::fbstring &resource);

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * PROPFIND request/response handler
 */
class WebDAVPROPFIND : public WebDAVRequest,
                       public std::enable_shared_from_this<WebDAVPROPFIND> {
public:
    WebDAVPROPFIND(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<PAPtr<pxml::Document>> operator()(
        const folly::fbstring &resource, const int depth,
        const folly::fbvector<folly::fbstring> &propFilter);

    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

    void onEOM() noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<PAPtr<pxml::Document>> m_resultPromise;
};

/**
 * PROPPATCH request/response handler
 */
class WebDAVPROPPATCH : public WebDAVRequest,
                        public std::enable_shared_from_this<WebDAVPROPPATCH> {
public:
    WebDAVPROPPATCH(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(const folly::fbstring &resource,
        const folly::fbstring &property, const folly::fbstring &value,
        const bool remove);

    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

    void onEOM() noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * DELETE request/response handler
 */
class WebDAVDELETE : public WebDAVRequest,
                     public std::enable_shared_from_this<WebDAVDELETE> {
public:
    WebDAVDELETE(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(const folly::fbstring &resource);

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * MOVE request/response handler
 */
class WebDAVMOVE : public WebDAVRequest,
                   public std::enable_shared_from_this<WebDAVMOVE> {
public:
    WebDAVMOVE(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(
        const folly::fbstring &resource, const folly::fbstring &destination);

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * COPY request/response handler
 */
class WebDAVCOPY : public WebDAVRequest,
                   public std::enable_shared_from_this<WebDAVCOPY> {
public:
    WebDAVCOPY(const WebDAVHelper &helper)
        : WebDAVRequest{helper}
    {
    }

    folly::Future<folly::Unit> operator()(
        const folly::fbstring &resource, const folly::fbstring &destination);

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

private:
    folly::Promise<folly::Unit> m_resultPromise;
};

/**
 * The @c FileHandle implementation for WebDAV storage helper.
 */
class WebDAVFileHandle : public FileHandle,
                         public std::enable_shared_from_this<WebDAVFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId WebDAV-specific ID associated with the file.
     * @param helper A pointer to the helper that created the handle.
     * @param ioCTX A reference to @c librados::IoCtx for async operations.
     */
    WebDAVFileHandle(
        folly::fbstring fileId, std::shared_ptr<WebDAVHelper> helper);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf) override;

    const Timeout &timeout() override;

private:
    std::shared_ptr<WebDAVHelper> m_helper;
    const folly::fbstring m_fileId;
};

/**
 * An implementation of @c StorageHelperFactory for WebDAV storage helper.
 */
class WebDAVHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    WebDAVHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
        LOG_FCALL();
    }

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters)
    {
        const auto &endpoint = getParam(parameters, "endpoint");
        const auto &verifyServerCertificateStr =
            getParam(parameters, "verifyServerCertificate", "true");
        const auto &credentialsTypeStr =
            getParam(parameters, "credentialsType", "basic");
        const auto &credentials = getParam(parameters, "credentials");
        const auto &authorizationHeader = getParam(
            parameters, "authorizationHeader", "Authorization: Bearer {}");
        const auto &rangeWriteSupportStr =
            getParam(parameters, "rangeWriteSupport", "none");

        Timeout timeout{getParam<std::size_t>(
            parameters, "timeout", ASYNC_OPS_TIMEOUT.count())};

        LOG_FCALL() << LOG_FARG(endpoint)
                    << LOG_FARG(verifyServerCertificateStr)
                    << LOG_FARG(credentials) << LOG_FARG(credentialsTypeStr)
                    << LOG_FARG(authorizationHeader)
                    << LOG_FARG(rangeWriteSupportStr);

        Poco::URI endpointUrl;

        constexpr auto kHTTPDefaultPort = 80;
        constexpr auto kHTTPSDefaultPort = 443;

        try {
            std::string scheme;

            if (endpoint.find(":") == folly::fbstring::npos) {
                // The endpoint does not contain neither scheme or port
                scheme = "http://";
            }
            else if (endpoint.find("http") != 0) {
                // The endpoint contains port but not a valid HTTP scheme
                if (endpoint.find(":443") == folly::fbstring::npos)
                    scheme = "http://";
                else
                    scheme = "https://";
            }

            endpointUrl = scheme + endpoint.toStdString();
        }
        catch (Poco::SyntaxException &e) {
            throw std::invalid_argument(
                "Invalid WebDAV endpoint: " + endpoint.toStdString());
        }

        if (endpointUrl.getHost().empty())
            throw std::invalid_argument(
                "Invalid WebDAV endpoint - missing hostname: " +
                endpoint.toStdString());

        if (endpointUrl.getScheme().empty()) {
            if (endpointUrl.getPort() == 0) {
                endpointUrl.setScheme("http");
                endpointUrl.setPort(kHTTPDefaultPort);
            }
            else if (endpointUrl.getPort() == kHTTPSDefaultPort) {
                endpointUrl.setScheme("https");
            }
            else {
                endpointUrl.setScheme("http");
            }
        }
        else if (endpointUrl.getScheme() != "http" &&
            endpointUrl.getScheme() != "https") {
            throw std::invalid_argument(
                "Invalid WebDAV endpoint - invalid scheme: " +
                endpointUrl.getScheme());
        }

        if (endpointUrl.getPort() == 0) {
            endpointUrl.setPort(endpointUrl.getScheme() == "https"
                    ? kHTTPSDefaultPort
                    : kHTTPDefaultPort);
        }

        bool verifyServerCertificate{true};
        if (verifyServerCertificateStr != "true")
            verifyServerCertificate = false;

        WebDAVCredentialsType credentialsType;
        if (credentialsTypeStr == "none")
            credentialsType = WebDAVCredentialsType::NONE;
        else if (credentialsTypeStr == "basic")
            credentialsType = WebDAVCredentialsType::BASIC;
        else if (credentialsTypeStr == "token")
            credentialsType = WebDAVCredentialsType::TOKEN;
        else
            throw std::invalid_argument("Invalid credentials type: " +
                credentialsTypeStr.toStdString());

        WebDAVRangeWriteSupport rangeWriteSupport;
        if (rangeWriteSupportStr.empty() || rangeWriteSupportStr == "none")
            rangeWriteSupport = WebDAVRangeWriteSupport::NONE;
        else if (rangeWriteSupportStr == "sabredav")
            rangeWriteSupport = WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE;
        else if (rangeWriteSupportStr == "moddav")
            rangeWriteSupport = WebDAVRangeWriteSupport::MODDAV_PUTRANGE;
        else
            throw std::invalid_argument(
                "Invalid range write support specified: " +
                rangeWriteSupportStr.toStdString());

        return std::make_shared<WebDAVHelper>(std::move(endpointUrl),
            verifyServerCertificate, credentialsType, credentials,
            authorizationHeader, rangeWriteSupport, m_executor, timeout);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one
