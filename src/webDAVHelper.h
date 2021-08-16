/**
 * @file webDAVHelper.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "helpers/storageHelper.h"
#include "webDAVHelperParams.h"

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

class WebDAVHelper;

constexpr auto kWebDAVHTTPVersionMajor = 1;
constexpr auto kWebDAVHTTPVersionMinor = 1;

constexpr auto kWebDAVRetryCount = 6;
const auto kWebDAVRetryMinimumDelay = std::chrono::milliseconds{5};

constexpr auto kNSDAV = "DAV:";
constexpr auto kNSOnedata = "http://onedata.org/metadata";

/**
 * HTTP Status Codes
 */
enum class WebDAVStatus : uint16_t {
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

class WebDAVHelper;
class WebDAVSession;

using WebDAVSessionPtr = std::unique_ptr<WebDAVSession>;
using WebDAVSessionPoolKey = std::tuple<folly::fbstring, uint16_t>;

/**
 * Handle 302 redirect by retrying the request.
 */
class WebDAVFoundException : public proxygen::HTTPException {
public:
    WebDAVFoundException(const std::string &locationHeader)
        : proxygen::HTTPException(proxygen::HTTPException::Direction::INGRESS,
              "302 redirect to " + locationHeader)
        , location{locationHeader}
    {
    }

    const std::string location;
};

struct WebDAVSessionPoolKeyCompare {
    bool equal(
        const WebDAVSessionPoolKey &a, const WebDAVSessionPoolKey &b) const
    {
        return a == b;
    }
    std::size_t hash(const WebDAVSessionPoolKey &a) const
    {
        return std::hash<WebDAVSessionPoolKey>()(a);
    }
};

/**
 * @c WebDAVSession holds structures related to a @c
 * proxygen::HTTPUpstreamSession running on a specific @c folly::EventBase
 */
struct WebDAVSession : public proxygen::HTTPSession::InfoCallback,
                       public proxygen::HTTPConnector::Callback {
    WebDAVHelper *helper{nullptr};
    proxygen::HTTPUpstreamSession *session{nullptr};
    folly::EventBase *evb{nullptr};
    std::unique_ptr<proxygen::HTTPConnector> connector;

    std::unique_ptr<folly::SharedPromise<folly::Unit>> connectionPromise;

    std::string host;

    WebDAVSessionPoolKey key;

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
    void onCreate(const proxygen::HTTPSessionBase &) override {}
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
    void onRead(const proxygen::HTTPSessionBase &, size_t bytesRead) override {}
    void onWrite(
        const proxygen::HTTPSessionBase &, size_t bytesWritten) override
    {
    }
    void onRequestBegin(const proxygen::HTTPSessionBase &) override {}
    void onRequestEnd(const proxygen::HTTPSessionBase &,
        uint32_t maxIngressQueueSize) override
    {
    }
    void onActivateConnection(const proxygen::HTTPSessionBase &) override {}
    void onDeactivateConnection(const proxygen::HTTPSessionBase &) override
    {
        LOG_DBG(4) << "Connection deactivated - restarting HTTP session";
    }
    void onDestroy(const proxygen::HTTPSessionBase &) override
    {
        LOG_DBG(4) << "Connection destroyed - restarting HTTP session";
        sessionValid = false;
    }
    void onIngressMessage(const proxygen::HTTPSessionBase &,
        const proxygen::HTTPMessage &) override
    {
    }
    void onIngressLimitExceeded(const proxygen::HTTPSessionBase &) override {}
    void onIngressPaused(const proxygen::HTTPSessionBase &) override {}
    void onTransactionDetached(const proxygen::HTTPSessionBase &) override {}
    void onPingReplySent(int64_t latency) override {}
    void onPingReplyReceived() override {}
    void onSettingsOutgoingStreamsFull(
        const proxygen::HTTPSessionBase &) override
    {
    }
    void onSettingsOutgoingStreamsNotFull(
        const proxygen::HTTPSessionBase &) override
    {
    }
    void onFlowControlWindowClosed(const proxygen::HTTPSessionBase &) override
    {
    }
    void onEgressBuffered(const proxygen::HTTPSessionBase &) override {}
    void onEgressBufferCleared(const proxygen::HTTPSessionBase &) override {}
    /**@}*/
};

/**
 * The WebDAVHelper class provides access to WebDAV storage via librados
 * library.
 */
class WebDAVHelper : public StorageHelper,
                     public std::enable_shared_from_this<WebDAVHelper> {
public:
    /**
     * Constructor.
     * @param executor Executor that will drive the helper's async
     * operations.
     */
    WebDAVHelper(std::shared_ptr<WebDAVHelperParams>,
        std::shared_ptr<folly::IOExecutor> executor,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER);

    /**
     * Destructor.
     * Closes connection to WebDAV storage cluster and destroys internal
     * context object.
     */
    ~WebDAVHelper();

    folly::fbstring name() const override { return WEBDAV_HELPER_NAME; };

    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;

    folly::Future<folly::Unit> access(const folly::fbstring &fileId,
        const int mask, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;

    folly::Future<struct stat> getattr(const folly::fbstring &fileId,
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<FileHandlePtr> open(
        const folly::fbstring &fileId, const int, const Params &) override;

    folly::Future<folly::Unit> unlink(
        const folly::fbstring &fileId, const size_t currentSize) override;

    folly::Future<folly::Unit> unlink(const folly::fbstring &fileId,
        const size_t currentSize, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId) override;

    folly::Future<folly::Unit> rmdir(const folly::fbstring &fileId,
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize) override;

    folly::Future<folly::Unit> truncate(const folly::fbstring &fileId,
        const off_t size, const size_t currentSize, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev) override;

    folly::Future<folly::Unit> mknod(const folly::fbstring &fileId,
        const mode_t mode, const FlagsSet &flags, const dev_t rdev,
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> mkdir(
        const folly::fbstring &fileId, const mode_t mode) override;

    folly::Future<folly::Unit> mkdir(const folly::fbstring &fileId,
        const mode_t mode, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> rename(
        const folly::fbstring &from, const folly::fbstring &to) override;

    folly::Future<folly::Unit> rename(const folly::fbstring &from,
        const folly::fbstring &to, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> chmod(
        const folly::fbstring &fileId, const mode_t mode) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> chown(const folly::fbstring &fileId,
        const uid_t uid, const gid_t gid) override
    {
        return folly::makeFuture();
    }

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count) override;

    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, off_t offset, size_t count,
        const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::fbstring> getxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> setxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace) override;

    folly::Future<folly::Unit> setxattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const folly::fbstring &value, bool create,
        bool replace, const int retryCount, const Poco::URI &redirectURL = {});

    folly::Future<folly::Unit> removexattr(
        const folly::fbstring &fileId, const folly::fbstring &name) override;

    folly::Future<folly::Unit> removexattr(const folly::fbstring &fileId,
        const folly::fbstring &name, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId) override;

    folly::Future<folly::fbvector<folly::fbstring>> listxattr(
        const folly::fbstring &fileId, const int retryCount,
        const Poco::URI &redirectURL = {});

    /**
     * Establishes connection to the WebDAV storage cluster.
     */
    folly::Future<WebDAVSession *> connect(WebDAVSessionPoolKey key = {});

    std::shared_ptr<folly::Executor> executor() { return m_executor; }

    WebDAVRangeWriteSupport rangeWriteSupport() const
    {
        return P()->rangeWriteSupport();
    }

    WebDAVCredentialsType credentialsType() const
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

    size_t maximumUploadSize() const { return P()->maximumUploadSize(); }

    uint32_t connectionPoolSize() const { return P()->connectionPoolSize(); }

    /**
     * Returns a WebDAVSession instance to the idle connection pool
     */
    void releaseSession(WebDAVSession *session)
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
    void initializeSessionPool(const WebDAVSessionPoolKey &key)
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

        spAcc->second = folly::fbvector<WebDAVSessionPtr>();

        m_idleSessionPool.insert(ispAcc, key);
        ispAcc->second =
            folly::MPMCQueue<WebDAVSession *, std::atomic, true>(100);

        for (auto i = 0u; i < P()->connectionPoolSize(); i++) {
            auto webDAVSession = std::make_unique<WebDAVSession>();
            webDAVSession->helper = this;
            webDAVSession->key = key;
            ispAcc->second.write(webDAVSession.get());
            spAcc->second.emplace_back(std::move(webDAVSession));
        }
    }

    std::shared_ptr<WebDAVHelperParams> P() const
    {
        return std::dynamic_pointer_cast<WebDAVHelperParams>(params().get());
    }

    struct WebDAVSessionThreadContext {
        folly::HHWheelTimer::UniquePtr timer;
    };

    folly::ThreadLocal<WebDAVSessionThreadContext> m_sessionContext;

    std::shared_ptr<folly::IOExecutor> m_executor;

    tbb::concurrent_hash_map<WebDAVSessionPoolKey,
        folly::MPMCQueue<WebDAVSession *, std::atomic, true>,
        WebDAVSessionPoolKeyCompare>
        m_idleSessionPool;
    tbb::concurrent_hash_map<WebDAVSessionPoolKey,
        folly::fbvector<WebDAVSessionPtr>, WebDAVSessionPoolKeyCompare>
        m_sessionPool;

    pxml::NamespaceSupport m_nsMap;
};

template <class T> using PAPtr = Poco::AutoPtr<T>;

/**
 * Base class for creating WebDAV request/response handlers
 */
class WebDAVRequest : public proxygen::HTTPTransactionHandler {
public:
    WebDAVRequest(WebDAVHelper *helper, WebDAVSession *session);

    virtual ~WebDAVRequest()
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

    WebDAVHelper *m_helper;
    WebDAVSession *m_session;
    std::shared_ptr<WebDAVHelperParams> m_params;
    proxygen::HTTPTransaction *m_txn;
    proxygen::HTTPMessage m_request;
    folly::fbstring m_path;
    Poco::URI m_redirectURL;

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
    WebDAVGET(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVPUT(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVPATCH(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVMKCOL(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVPROPFIND(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVPROPPATCH(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVDELETE(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVMOVE(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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
    WebDAVCOPY(WebDAVHelper *helper, WebDAVSession *session)
        : WebDAVRequest{helper, session}
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

    folly::Future<folly::IOBufQueue> read(const off_t offset,
        const std::size_t size, const int retryCount,
        const Poco::URI &redirectURL = {});

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        WriteCallback &&writeCb) override;

    folly::Future<std::size_t> write(const off_t offset, folly::IOBufQueue buf,
        const int retryCount, const Poco::URI &redirectURL = {});

    const Timeout &timeout() override;

private:
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

    virtual folly::fbstring name() const override { return WEBDAV_HELPER_NAME; }

    std::vector<folly::fbstring> overridableParams() const override
    {
        return {"endpoint", "verifyServerCertificate", "connectionPoolSize",
            "timeout"};
    };

    std::shared_ptr<StorageHelper> createStorageHelper(const Params &parameters,
        ExecutionContext executionContext = ExecutionContext::ONEPROVIDER)
    {
        return std::make_shared<WebDAVHelper>(
            WebDAVHelperParams::create(parameters), m_executor,
            executionContext);
    }

private:
    std::shared_ptr<folly::IOExecutor> m_executor;
};

} // namespace helpers
} // namespace one
