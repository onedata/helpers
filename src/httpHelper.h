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

#include "asioExecutor.h"
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
#include <folly/MPMCQueue.h>
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

namespace pxml = Poco::XML;

namespace one {
namespace helpers {

class HTTPHelper;

constexpr auto kHTTPVersionMajor = 1;
constexpr auto kHTTPVersionMinor = 1;

constexpr auto kHTTPRetryCount = 6;
const auto kHTTPRetryMinimumDelay = std::chrono::milliseconds{5};

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
class HTTPSession;

using HTTPSessionPtr = std::unique_ptr<HTTPSession>;
using HTTPSessionPoolKey = std::tuple<folly::fbstring, uint16_t, bool, bool>;

/**
 * Handle 302 redirect by retrying the request.
 */
class HTTPFoundException : public proxygen::HTTPException {
public:
    HTTPFoundException(const std::string &locationHeader)
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
struct HTTPSession : public proxygen::HTTPSession::InfoCallback,
                     public proxygen::HTTPConnector::Callback {
    HTTPHelper *helper{nullptr};
    proxygen::HTTPUpstreamSession *session{nullptr};
    folly::EventBase *evb{nullptr};
    std::unique_ptr<proxygen::HTTPConnector> connector;

    std::unique_ptr<folly::SharedPromise<folly::Unit>> connectionPromise;

    std::string host;

    HTTPSessionPoolKey key;

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
    void onCreate(const proxygen::HTTPSession &) override {}
    void onIngressError(
        const proxygen::HTTPSession &s, proxygen::ProxygenError e) override
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
    void onRead(const proxygen::HTTPSession &, size_t bytesRead) override {}
    void onWrite(const proxygen::HTTPSession &, size_t bytesWritten) override {}
    void onRequestBegin(const proxygen::HTTPSession &) override {}
    void onRequestEnd(
        const proxygen::HTTPSession &, uint32_t maxIngressQueueSize) override
    {
    }
    void onActivateConnection(const proxygen::HTTPSession &) override {}
    void onDeactivateConnection(const proxygen::HTTPSession &) override
    {
        LOG_DBG(4) << "Connection deactivated - restarting HTTP session";
    }
    void onDestroy(const proxygen::HTTPSession &) override
    {
        LOG_DBG(4) << "Connection destroyed - restarting HTTP session";
        sessionValid = false;
    }
    void onIngressMessage(
        const proxygen::HTTPSession &, const proxygen::HTTPMessage &) override
    {
    }
    void onIngressLimitExceeded(const proxygen::HTTPSession &) override {}
    void onIngressPaused(const proxygen::HTTPSession &) override {}
    void onTransactionDetached(const proxygen::HTTPSession &) override {}
    void onPingReplySent(int64_t latency) override {}
    void onPingReplyReceived() override {}
    void onSettingsOutgoingStreamsFull(const proxygen::HTTPSession &) override
    {
    }
    void onSettingsOutgoingStreamsNotFull(
        const proxygen::HTTPSession &) override
    {
    }
    void onFlowControlWindowClosed(const proxygen::HTTPSession &) override {}
    void onEgressBuffered(const proxygen::HTTPSession &) override {}
    void onEgressBufferCleared(const proxygen::HTTPSession &) override {}
    /**@}*/
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
        std::shared_ptr<folly::IOExecutor> executor);

    /**
     * Destructor.
     * Closes connection to HTTP storage cluster and destroys internal
     * context object.
     */
    ~HTTPHelper();

    folly::fbstring name() const override { return HTTP_HELPER_NAME; };

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId,
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<FileHandlePtr> open(
        const folly::fbstring &fileId, const int, const Params &) override;

    /**
     * Establishes connection to the HTTP storage cluster.
     */
    folly::Future<HTTPSession *> connect(HTTPSessionPoolKey key = {});

    std::shared_ptr<folly::Executor> executor() { return m_executor; }

    HTTPCredentialsType credentialsType() const
    {
        return P()->credentialsType();
    }

    Poco::URI endpoint() const { return P()->endpoint(); }

    folly::fbstring credentials() const { return P()->credentials(); }

    folly::fbstring oauth2IdP() const { return P()->oauth2IdP(); }

    folly::fbstring authorizationHeader() const
    {
        return P()->authorizationHeader();
    }

    folly::fbstring accessToken() const { return P()->accessToken(); }

    uint32_t connectionPoolSize() const { return P()->connectionPoolSize(); }

    /**
     * Returns a HTTPSession instance to the idle connection pool
     */
    void releaseSession(HTTPSession *session)
    {
        decltype(m_idleSessionPool)::accessor ispAcc;
        if (m_idleSessionPool.find(ispAcc, session->key))
            ispAcc->second.write(std::move(session));
    };

    bool setupOpenSSLCABundlePath(SSL_CTX *ctx);

    /**
     * In case credentials are provisioned by OAuth2 IdP, this method checks
     * if the current access token is still valid based on TTL received
     * when creating the helper.
     * @return true if access token is still valid
     */
    bool isAccessTokenValid() const;

private:
    void initializeSessionPool(const HTTPSessionPoolKey &key)
    {
        constexpr auto kHTTPSessionPoolSize = 1024u;

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
        ispAcc->second =
            folly::MPMCQueue<HTTPSession *, std::atomic, true>(kHTTPSessionPoolSize);

        for (auto i = 0u; i < P()->connectionPoolSize(); i++) {
            auto httpSession = std::make_unique<HTTPSession>();
            httpSession->helper = this;
            httpSession->key = key;
            ispAcc->second.write(httpSession.get());
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
        folly::MPMCQueue<HTTPSession *, std::atomic, true>,
        HTTPSessionPoolKeyCompare>
        m_idleSessionPool;
    tbb::concurrent_hash_map<HTTPSessionPoolKey,
        folly::fbvector<HTTPSessionPtr>, HTTPSessionPoolKeyCompare>
        m_sessionPool;

    pxml::NamespaceSupport m_nsMap;
};

template <class T> using PAPtr = Poco::AutoPtr<T>;

/**
 * Base class for creating HTTP request/response handlers
 */
class HTTPRequest : public proxygen::HTTPTransactionHandler {
public:
    HTTPRequest(HTTPHelper *helper, HTTPSession *session);

    virtual ~HTTPRequest()
    {
        // In case the session was not released after completion of request or
        // in error callback - release it here
        if (m_session != nullptr) {
            m_helper->releaseSession(std::move(m_session));
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

    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;
    void onEOM() noexcept override;
    void onError(const proxygen::HTTPException &error) noexcept override;

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
    HTTPFileHandle(folly::fbstring fileId, std::shared_ptr<HTTPHelper> helper);

    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<folly::IOBufQueue> read(const off_t offset,
        const std::size_t size, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf) override
    {
        throw std::system_error{
            std::make_error_code(std::errc::function_not_supported)};
    }

    const Timeout &timeout() override;

private:
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
    HTTPHelperFactory(std::shared_ptr<folly::IOExecutor> executor)
        : m_executor{std::move(executor)}
    {
        LOG_FCALL();
    }

    virtual folly::fbstring name() const override { return HTTP_HELPER_NAME; }

    const std::vector<folly::fbstring> overridableParams() const override
    {
        return {"endpoint", "verifyServerCertificate", "connectionPoolSize",
            "timeout", "credentialsType", "credentials"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters)
    {
        return std::make_shared<HTTPHelper>(
            HTTPHelperParams::create(parameters), m_executor);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one
