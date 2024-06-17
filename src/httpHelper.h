/**
 * @file httpHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"
#include "httpHelperParams.h"

#include "helpers/logging.h"

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
#include <folly/Executor.h>
#include <folly/ThreadLocal.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/executors/IOExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/SharedPromise.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPSession.h>
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <proxygen/lib/utils/Base64.h>
#include <proxygen/lib/utils/URL.h>
#include <tbb/concurrent_priority_queue.h>

namespace pxml = Poco::XML;

namespace one {
namespace helpers {

class HTTPHelper;

constexpr auto kHTTPVersionMajor = 1;
constexpr auto kHTTPVersionMinor = 1;

constexpr auto kHTTPRetryCount = 6;
const auto kHTTPRetryMinimumDelay = std::chrono::milliseconds{5}; // NOLINT

/**
 * HTTP Status Codes
 */
enum class HTTPStatus : uint16_t {
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

class HTTPHelper;
struct HTTPSession;

using HTTPSessionPtr = std::unique_ptr<HTTPSession>;
// Session key <host, port, isExternal, isHttps>
using HTTPSessionPoolKey = std::tuple<folly::fbstring, uint16_t, bool, bool>;

/**
 * Handle 302 redirect by retrying the request.
 */
class HTTPFoundException : public proxygen::HTTPException {
public:
    explicit HTTPFoundException(const std::string &locationHeader)
        : proxygen::HTTPException(proxygen::HTTPException::Direction::INGRESS,
              "302 redirect to " + locationHeader)
        , location{locationHeader}
    {
    }

    const std::string location;
};

struct HTTPSessionPoolKeyCompare {
    bool equal(const HTTPSessionPoolKey &a, const HTTPSessionPoolKey &b) const
    {
        return a == b;
    }
    std::size_t hash(const HTTPSessionPoolKey &a) const
    {
        return std::hash<HTTPSessionPoolKey>()(a);
    }
};

/**
 * @c HTTPSession holds structures related to a @c
 * proxygen::HTTPUpstreamSession running on a specific @c folly::EventBase
 */
struct HTTPSession : public proxygen::HTTPSessionBase::InfoCallback,
                     public proxygen::HTTPConnector::Callback {
    HTTPHelper *helper{nullptr};
    proxygen::HTTPUpstreamSession *session{nullptr};
    folly::EventBase *evb{nullptr};
    std::unique_ptr<proxygen::HTTPConnector> connector;

    // Shared promise ensuring that only one connection per session is initiated
    // at the same time
    std::unique_ptr<folly::SharedPromise<folly::Unit>> connectionPromise;

    // Session key allowing connections to multiple hosts
    HTTPSessionPoolKey key;
    folly::SocketAddress address;

    // Set to true when server returned 'Connection: close' header in response
    // to a request
    bool closedByRemote{false};

    // Set to true after the connectSuccess callback
    bool sessionValid{false};

    void reset();

    /**
     * \defgroup proxygen::HTTPConnector methods
     * @{
     */
    void connectSuccess(
        proxygen::HTTPUpstreamSession *reconnectedSession) override;
    void connectError(const folly::AsyncSocketException &ex) override;
    /**@}*/

    /**
     * \defgroup proxygen::HTTPSession::InfoCallback methods
     * @{
     */
    void onCreate(const proxygen::HTTPSessionBase & /*base*/) override { }
    void onIngressError(
        const proxygen::HTTPSessionBase &s, proxygen::ProxygenError e) override
    {
        LOG_DBG(4) << "Ingress Error - restarting HTTP session: "
                   << proxygen::getErrorString(e);
        sessionValid = false;
    }
    void onIngressEOF() override
    {
        LOG_DBG(4) << "Ingress EOF - restarting HTTP session";

        sessionValid = false;
    }
    void onRead(
        const proxygen::HTTPSessionBase & /*base*/, size_t bytesRead) override
    {
    }
    void onWrite(const proxygen::HTTPSessionBase & /*base*/,
        size_t bytesWritten) override
    {
    }
    void onRequestBegin(const proxygen::HTTPSessionBase & /*base*/) override { }
    void onRequestEnd(const proxygen::HTTPSessionBase & /*base*/,
        uint32_t maxIngressQueueSize) override
    {
    }
    void onActivateConnection(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onDeactivateConnection(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
        LOG_DBG(4) << "Connection deactivated - restarting HTTP session";
    }
    void onDestroy(const proxygen::HTTPSessionBase & /*base*/) override
    {
        LOG_DBG(4) << "Connection destroyed - restarting HTTP session";
        sessionValid = false;
    }
    void onIngressMessage(const proxygen::HTTPSessionBase & /*base*/,
        const proxygen::HTTPMessage & /*msg*/) override
    {
    }
    void onIngressLimitExceeded(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onIngressPaused(const proxygen::HTTPSessionBase & /*base*/) override {
    }
    void onTransactionDetached(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onPingReplySent(int64_t latency) override { }
    void onPingReplyReceived() override { }
    void onSettingsOutgoingStreamsFull(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onSettingsOutgoingStreamsNotFull(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onFlowControlWindowClosed(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onEgressBuffered(const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    void onEgressBufferCleared(
        const proxygen::HTTPSessionBase & /*base*/) override
    {
    }
    /**@}*/
};

/**
 * Sort functor which ensures that the idle connection queue keeps
 * active free connections at the head of the queue, to minimize
 * number of concurrent active connections.
 */
struct HTTPSessionPriorityCompare {
    bool operator()(const HTTPSession *a, const HTTPSession *b) const
    {
        return a->sessionValid < b->sessionValid; // NOLINT
    }
};

/**
 * The HTTPHelper class provides access to HTTP storage via librados
 * library.
 */
class HTTPHelper : public StorageHelper,
                   public std::enable_shared_from_this<HTTPHelper> {
public:
    /**
     * Constructor.
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    HTTPHelper(std::shared_ptr<HTTPHelperParams>,
        std::shared_ptr<folly::IOExecutor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    HTTPHelper(const HTTPHelper &) = delete;
    HTTPHelper &operator=(const HTTPHelper &) = delete;
    HTTPHelper(HTTPHelper &&) = delete;
    HTTPHelper &operator=(HTTPHelper &&) = delete;

    /**
     * Destructor.
     * Closes connection to HTTP storage cluster and destroys internal
     * context object.
     */
    virtual ~HTTPHelper(); // NOLINT

    folly::fbstring name() const override { return HTTP_HELPER_NAME; };

    folly::Future<folly::Unit> checkStorageAvailability() override;

    folly::Future<folly::Unit> options();

    folly::Future<folly::Unit> options(
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId,
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int /*flags*/, const Params & /*openParams*/) override;

    /**
     * Establishes connection to the HTTP storage cluster.
     */
    folly::Future<HTTPSession *> connect(HTTPSessionPoolKey key = {});

    std::shared_ptr<folly::Executor> executor() override { return m_executor; }

    HTTPCredentialsType credentialsType() const
    {
        return P()->credentialsType();
    }

    std::pair<HTTPSessionPoolKey, folly::fbstring> relativizeURI(
        const folly::fbstring &fileId) const;

    Poco::URI endpoint() const { return P()->endpoint(); }

    folly::fbstring credentials() const { return P()->credentials(); }

    folly::fbstring oauth2IdP() const { return P()->oauth2IdP(); }

    folly::fbstring authorizationHeader() const
    {
        return P()->authorizationHeader();
    }

    folly::fbstring accessToken() const { return P()->accessToken(); }

    folly::fbstring hostHeader() const
    {
        return fmt::format(
            "{}:{}", P()->endpoint().getHost(), P()->endpoint().getPort());
    }

    uint32_t connectionPoolSize() const { return P()->connectionPoolSize(); }

    /**
     * Returns a HTTPSession instance to the idle connection pool
     */
    void releaseSession(HTTPSession *session)
    {
        decltype(m_idleSessionPool)::accessor ispAcc;
        if (m_idleSessionPool.find(ispAcc, session->key)) {
            ispAcc->second.emplace(session);
        }
    };

    static bool setupOpenSSLCABundlePath(SSL_CTX *ctx);

    /**
     * In case credentials are provisioned by OAuth2 IdP, this method checks
     * if the current access token is still valid based on TTL received
     * when creating the helper.
     * @return true if access token is still valid
     */
    bool isAccessTokenValid() const;

    void addCookie(const std::string &host, const std::string &cookie);

    void clearCookies(const std::string &host);

    std::vector<std::string> cookies(const std::string &host) const;

private:
    void initializeSessionPool(const HTTPSessionPoolKey &key)
    {
        // Initialize HTTP session pool with connection to the
        // main host:port as defined in the helper parameters
        // Since some requests may create redirects, these will
        // be added to the session pool with different keys
        decltype(m_sessionPool)::accessor spAcc;
        decltype(m_idleSessionPool)::accessor ispAcc;

        auto inserted = m_sessionPool.insert(spAcc, key);

        if (!inserted)
            // Pool for this key already exists
            return;

        spAcc->second = folly::fbvector<HTTPSessionPtr>();

        m_idleSessionPool.insert(ispAcc, key);

        for (auto i = 0U; i < P()->connectionPoolSize(); i++) {
            auto httpSession = std::make_unique<HTTPSession>();
            httpSession->helper = this;
            httpSession->key = key;
            httpSession->address = folly::SocketAddress{};
            ispAcc->second.emplace(httpSession.get());
            spAcc->second.emplace_back(std::move(httpSession));
        }
    }

    std::shared_ptr<HTTPHelperParams> P() const
    {
        return std::dynamic_pointer_cast<HTTPHelperParams>(params().get());
    }

    struct HTTPSessionThreadContext {
        folly::HHWheelTimer::UniquePtr timer;
    };

    folly::ThreadLocal<HTTPSessionThreadContext> m_sessionContext;

    std::shared_ptr<folly::IOExecutor> m_executor;

    tbb::concurrent_hash_map<HTTPSessionPoolKey,
        tbb::concurrent_priority_queue<HTTPSession *,
            HTTPSessionPriorityCompare>,
        HTTPSessionPoolKeyCompare>
        m_idleSessionPool;
    tbb::concurrent_hash_map<HTTPSessionPoolKey,
        folly::fbvector<HTTPSessionPtr>, HTTPSessionPoolKeyCompare>
        m_sessionPool;

    pxml::NamespaceSupport m_nsMap;

    // Cookie jar indexed by hostname
    tbb::concurrent_hash_map<std::string, std::vector<std::string>> m_cookies;
};

template <class T> using PAPtr = Poco::AutoPtr<T>;

/**
 * Base class for creating HTTP request/response handlers
 */
class HTTPRequest : public proxygen::HTTPTransactionHandler {
public:
    HTTPRequest(HTTPHelper *helper, HTTPSession *session);

    HTTPRequest(const HTTPRequest &) = delete;
    HTTPRequest &operator=(const HTTPRequest &) = delete;
    HTTPRequest(HTTPRequest &&) = delete;
    HTTPRequest &operator=(HTTPRequest &&) = delete;

    virtual ~HTTPRequest() // NOLINT
    {
        // In case the session was not released after completion of request or
        // in error callback - release it here
        if (m_session != nullptr) {
            m_helper->releaseSession(std::move(m_session)); // NOLINT
        }
    };

    void setRedirectURL(const Poco::URI &redirectURL)
    {
        m_redirectURL = redirectURL;
    }

    Poco::URI redirectURL() const { return m_redirectURL; }

    folly::Future<proxygen::HTTPTransaction *> startTransaction();

    folly::EventBase *eventBase() const { return m_session->evb; }

    proxygen::HTTPMessage &request() { return m_request; }

    virtual void processHeaders(
        const std::unique_ptr<proxygen::HTTPMessage> &msg) noexcept {};

    /**
     * \defgroup proxygen::HTTPTransactionHandler methods
     * @{
     */
    void setTransaction(proxygen::HTTPTransaction *txn) noexcept override;
    void detachTransaction() noexcept override;
    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override;
    void onTrailers(
        std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept override;
    void onEOM() noexcept override;
    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override = 0;
    void onEgressPaused() noexcept override;
    void onEgressResumed() noexcept override;
    /**@{*/

protected:
    void updateRequestURL(const folly::fbstring &resource);

    HTTPHelper *m_helper;
    HTTPSession *m_session;
    std::shared_ptr<HTTPHelperParams> m_params;
    proxygen::HTTPTransaction *m_txn;
    proxygen::HTTPMessage m_request;
    folly::fbstring m_path;
    Poco::URI m_redirectURL;

    uint16_t m_resultCode;
    std::unique_ptr<folly::IOBufQueue> m_resultBody;

    // Make sure the request instance is not destroyed before Proxygen
    // transaction is detached
    std::shared_ptr<HTTPRequest> m_destructionGuard;
};

/**
 * GET request/response handler
 */
class HTTPGET : public HTTPRequest,
                public std::enable_shared_from_this<HTTPGET> {
public:
    HTTPGET(HTTPHelper *helper, HTTPSession *session)
        : HTTPRequest{helper, session}
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
 * OPTIONS request/response handler
 */
class HTTPOPTIONS : public HTTPRequest,
                    public std::enable_shared_from_this<HTTPOPTIONS> {
public:
    HTTPOPTIONS(HTTPHelper *helper, HTTPSession *session)
        : HTTPRequest{helper, session}
    {
    }

    folly::Future<std::map<folly::fbstring, folly::fbstring>> operator()();

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

    void processHeaders(
        const std::unique_ptr<proxygen::HTTPMessage> &msg) noexcept override;

private:
    folly::Promise<std::map<folly::fbstring, folly::fbstring>> m_resultPromise;
};

/**
 * HEAD request/response handler
 */
class HTTPHEAD : public HTTPRequest,
                 public std::enable_shared_from_this<HTTPHEAD> {
public:
    HTTPHEAD(HTTPHelper *helper, HTTPSession *session)
        : HTTPRequest{helper, session}
    {
    }

    folly::Future<std::map<folly::fbstring, folly::fbstring>> operator()(
        const folly::fbstring &resource);

    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

    void processHeaders(
        const std::unique_ptr<proxygen::HTTPMessage> &msg) noexcept override;

private:
    folly::Promise<std::map<folly::fbstring, folly::fbstring>> m_resultPromise;
};

/**
 * The @c FileHandle implementation for HTTP storage helper.
 */
class HTTPFileHandle : public FileHandle,
                       public std::enable_shared_from_this<HTTPFileHandle> {
public:
    /**
     * Constructor.
     * @param fileId HTTP-specific ID associated with the file.
     * @param helper A pointer to the helper that created the handle.
     * @param ioCTX A reference to @c librados::IoCtx for async operations.
     */
    HTTPFileHandle(
        const folly::fbstring &fileId, std::shared_ptr<HTTPHelper> helper);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(const off_t /*offset*/,
        folly::IOBufQueue /*buf*/, WriteCallback && /*writeCb*/) override
    {
        throw std::system_error{
            std::make_error_code(std::errc::function_not_supported)};
    }

    const Timeout &timeout() override;

private:
    folly::Future<folly::IOBufQueue> read(const off_t offset,
        const std::size_t size, const int retryCount,
        const Poco::URI &redirectURL);

    folly::fbstring m_fileId;
    folly::fbstring m_effectiveFileId;
    HTTPSessionPoolKey m_sessionPoolKey;
};

/**
 * An implementation of @c StorageHelperFactory for HTTP storage helper.
 */
class HTTPHelperFactory : public StorageHelperFactory {
public:
    /**
     * Constructor.
     * @param service @c io_service that will be used for some async
     * operations.
     */
    explicit HTTPHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
        LOG_FCALL();
    }

    folly::fbstring name() const override { return HTTP_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"endpoint", "verifyServerCertificate", "connectionPoolSize",
            "timeout", "credentialsType", "credentials"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters, ExecutionContext executionContext) override
    {
        return std::make_shared<HTTPHelper>(
            HTTPHelperParams::create(parameters), m_executor, executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one
