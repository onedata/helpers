/**
 * @file httpHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "httpHelper.h"
#include "helpers/logging.h"
#include "monitoring/monitoring.h"

#include <Poco/DOM/NodeList.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeParser.h>
#include <Poco/SAX/NamespaceSupport.h>
#include <Poco/StringTokenizer.h>
#include <Poco/URI.h>
#include <Poco/UTF8Encoding.h>
#include <boost/filesystem.hpp>
#include <folly/Format.h>
#include <folly/SocketAddress.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/HHWheelTimer.h>
#include <folly/io/async/SSLOptions.h>
#include <glog/stl_logging.h>
#include <openssl/ssl.h>

#include <functional>

namespace one {
namespace helpers {

namespace {
/**
 * Convert HTTP Status Code to appropriate POSIX error
 */
int httpStatusToPosixError(uint16_t httpStatus)
{
    const auto kHTTPStatusDivider = 100;
    if (httpStatus / kHTTPStatusDivider == 2)
        return 0;

    if (httpStatus / kHTTPStatusDivider < 4)
        return EIO;

    switch (static_cast<HTTPStatus>(httpStatus)) {
        case HTTPStatus::BadRequest:
            return EBADMSG;
        case HTTPStatus::Unauthorized:
        case HTTPStatus::Forbidden:
            return EPERM;
        case HTTPStatus::NotFound:
            return ENOENT;
        case HTTPStatus::MethodNotAllowed:
            return ENOTSUP;
        case HTTPStatus::NotAcceptable:
        case HTTPStatus::ProxyAuthenticationRequired:
            return EACCES;
        case HTTPStatus::RequestTimeout:
            return EAGAIN;
        case HTTPStatus::Conflict:
            return EBADMSG;
        case HTTPStatus::Gone:
            return ENXIO;
        case HTTPStatus::LengthRequired:
        case HTTPStatus::PreconditionFailed:
            return EINVAL;
        case HTTPStatus::PayloadTooLarge:
            return EFBIG;
        case HTTPStatus::URITooLong:
        case HTTPStatus::UnsupportedMediaType:
            return EINVAL;
        case HTTPStatus::RangeNotSatisfiable:
            return ERANGE;
        case HTTPStatus::ExpectationFailed:
        case HTTPStatus::UpgradeRequired:
        case HTTPStatus::PreconditionRequired:
            return EINVAL;
        case HTTPStatus::TooManyRequests:
            return EBUSY;
        case HTTPStatus::RequestHeaderFieldsTooLarge:
            return EFBIG;
        case HTTPStatus::UnavailableForLegalReasons:
            return EPERM;
        case HTTPStatus::InternalServerError:
            return EIO;
        case HTTPStatus::NotImplemented:
            return ENOTSUP;
        case HTTPStatus::BadGateway:
        case HTTPStatus::ServiceUnavailable:
            return ENXIO;
        case HTTPStatus::GatewayTimeout:
            return EAGAIN;
        case HTTPStatus::HTTPVersionNotSupported:
            return EINVAL;
        case HTTPStatus::NetworkAuthenticationRequired:
            return EACCES;
        default:
            return EIO;
    }
}

// Retry only in case one of these errors occured
const std::set<int> &HTTPRetryErrors()
{
    static const std::set<int> HTTP_RETRY_ERRORS = {EINTR, EIO, EAGAIN, EACCES,
        EBUSY, EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK, EWOULDBLOCK,
        ENONET, ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN, ENETUNREACH,
        ECONNABORTED, ECONNRESET, ENOTCONN, EHOSTDOWN, EHOSTUNREACH, EREMOTEIO,
        ENOMEDIUM, ECANCELED};
    return HTTP_RETRY_ERRORS;
}

inline bool shouldRetryError(int ec)
{
    return HTTPRetryErrors().find(ec) != HTTPRetryErrors().cend();
}

inline auto retryDelay(int retriesLeft)
{
    const unsigned int kHTTPRetryBaseDelay_ms = 100;
    return kHTTPRetryMinimumDelay +
        std::chrono::milliseconds{kHTTPRetryBaseDelay_ms *
            (kHTTPRetryCount - retriesLeft) * (kHTTPRetryCount - retriesLeft)};
}

inline std::string ensureHttpPath(const folly::fbstring &path)
{
    if (path.empty())
        return "/";

    auto result = folly::trimWhitespace(path);

    if (result[0] != '/')
        return folly::sformat("/{}", result);

    if (result.subpiece(0, 2) == "//")
        return folly::sformat("{}", result.subpiece(1));

    return folly::sformat("{}", result);
}
} // namespace

void HTTPSession::reset()
{
    sessionValid = false;
    closedByRemote = false;
    session = nullptr;
    connectionPromise = std::make_unique<folly::SharedPromise<folly::Unit>>();

    ONE_METRIC_COUNTER_DEC("comp.helpers.mod.http.connections.active")
}

HTTPFileHandle::HTTPFileHandle(
    const folly::fbstring &fileId, std::shared_ptr<HTTPHelper> helper)
    : FileHandle{fileId, std::move(helper)}
    , m_fileId{fileId}
    , m_effectiveFileId{fileId}
    , m_sessionPoolKey{}
{
    LOG_FCALL() << LOG_FARG(fileId);

    // Try to parse the fileId as URL - if it contains Host - treat it
    // as an external resource and create a separate HTTP session pool key
    auto poolKeyAndUri = std::dynamic_pointer_cast<HTTPHelper>(this->helper())
                             ->relativizeURI(fileId);
    m_sessionPoolKey = std::move(poolKeyAndUri.first);
    m_effectiveFileId = std::move(poolKeyAndUri.second);
}

folly::Future<folly::IOBufQueue> HTTPFileHandle::read(
    const off_t offset, const std::size_t size)
{
    return read(offset, size, kHTTPRetryCount, {});
}

folly::Future<folly::IOBufQueue> HTTPFileHandle::read(const off_t offset,
    const std::size_t size, const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.http.read");

    auto helper = std::dynamic_pointer_cast<HTTPHelper>(this->helper());

    auto sessionPoolKey = m_sessionPoolKey;
    auto effectiveFileId = m_effectiveFileId;

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey = HTTPSessionPoolKey{redirectURL.getHost(),
            redirectURL.getPort(), false, redirectURL.getScheme() == "https"};
        effectiveFileId = redirectURL.getPath();
    }

    return helper->connect(sessionPoolKey)
        .thenValue([fileId = effectiveFileId, redirectURL, offset, size,
                       retryCount, timer = std::move(timer), helper,
                       self = shared_from_this()](
                       HTTPSession *session) mutable {
            auto getRequest = std::make_shared<HTTPGET>(helper.get(), session);

            if (!redirectURL.empty()) {
                getRequest->setRedirectURL(redirectURL);
            }

            return (*getRequest)(fileId, offset, size)
                .thenError(folly::tag_t<HTTPFoundException>{},
                    [fileId, self, offset, size, retryCount](auto &&redirect) {
                        LOG_DBG(2) << "Redirecting HTTP read request of file "
                                   << fileId << " to: " << redirect.location;
                        return self->read(offset, size, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [self, fileId, helper, offset, size, retryCount](auto &&e) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.http.read.retries");

                            LOG_DBG(1) << "Retrying HTTP read request for "
                                       << fileId << " due to " << e.what();
                            return folly::makeFuture()
                                .delayed(retryDelay(retryCount))
                                .thenValue([self, offset, size, retryCount](
                                               auto && /*unit*/) {
                                    return self->read(
                                        offset, size, retryCount - 1, {});
                                });
                        }

                        LOG_DBG(1) << "Failed HTTP read request for " << fileId
                                   << " due to " << e.what();
                        return makeFuturePosixException<folly::IOBufQueue>(EIO);
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [self, fileId, helper, offset, size, retryCount](auto &&e) {
                        if (e.code().value() == ERANGE) {
                            return folly::makeFuture<folly::IOBufQueue>(
                                folly::IOBufQueue(
                                    folly::IOBufQueue::cacheChainLength()));
                        }

                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.http.read.retries");

                            LOG_DBG(1) << "Retrying HTTP read request for "
                                       << fileId << " due to " << e.what();
                            return folly::makeFuture()
                                .delayed(retryDelay(retryCount))
                                .thenValue([self, offset, size, retryCount](
                                               auto && /*unit*/) {
                                    return self->read(
                                        offset, size, retryCount - 1, {});
                                });
                        }

                        LOG_DBG(1) << "Failed HTTP read request for " << fileId
                                   << " due to " << e.what();
                        return makeFuturePosixException<folly::IOBufQueue>(
                            e.code().value());
                    })
                .thenValue([timer = std::move(timer), getRequest, helper](
                               folly::IOBufQueue &&buf) {
                    ONE_METRIC_TIMERCTX_STOP(timer, buf.chainLength());
                    return std::move(buf);
                });
        });
}

const Timeout &HTTPFileHandle::timeout() { return helper()->timeout(); }

HTTPHelper::HTTPHelper(std::shared_ptr<HTTPHelperParams> params,
    std::shared_ptr<folly::IOExecutor> executor,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_executor{std::move(executor)}
{
    invalidateParams()->setValue(std::move(params));

    initializeSessionPool(
        HTTPSessionPoolKey{P()->endpoint().getHost(), P()->endpoint().getPort(),
            false, P()->endpoint().getScheme() == "https"});
}

HTTPHelper::~HTTPHelper()
{
    LOG_FCALL();

    // Close any pending sessions
    for (auto const &pool : m_sessionPool) {
        for (const auto &s : pool.second) {
            if (s->session != nullptr && s->evb != nullptr) {
                s->evb->runInEventBaseThreadAndWait([session = s->session] {
                    session->setInfoCallback(nullptr);
                });
            }
        }
    }
}

bool HTTPHelper::isAccessTokenValid() const
{
    // Refresh the token as soon as the token will be valid for
    // less than 60 seconds
    constexpr auto kHTTPAccessTokenMinimumTTL = 60;
    std::chrono::seconds httpAccessTokenMinimumTTL{kHTTPAccessTokenMinimumTTL};

    if (P()->testTokenRefreshMode())
        httpAccessTokenMinimumTTL = std::chrono::seconds{0};

    if (P()->credentialsType() == HTTPCredentialsType::OAUTH2) {
        LOG_DBG(3) << "Checking HTTP access token ttl: "
                   << P()->accessTokenTTL().count() << " left: "
                   << P()->accessTokenTTL().count() -
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now() - P()->createdOn())
                    .count();

        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now() - P()->createdOn()) <
            P()->accessTokenTTL() - httpAccessTokenMinimumTTL;
    }

    return true;
}

folly::Future<FileHandlePtr> HTTPHelper::open(const folly::fbstring &fileId,
    const int /*flags*/, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto handle = std::make_shared<HTTPFileHandle>(fileId, shared_from_this());

    return folly::makeFuture(handle);
}

std::pair<HTTPSessionPoolKey, folly::fbstring> HTTPHelper::relativizeURI(
    const folly::fbstring &fileId) const
{
    std::pair<HTTPSessionPoolKey, folly::fbstring> res;

    HTTPSessionPoolKey sessionPoolKey{};
    folly::fbstring effectiveFileId{fileId};

    auto endpoint = this->endpoint();
    auto fileURI = Poco::URI(fileId.toStdString());
    if (!fileURI.getHost().empty()) {
        if (fileURI.getHost() == endpoint.getHost() &&
            fileURI.getPort() == endpoint.getPort() &&
            fileURI.getScheme() == endpoint.getScheme()) {
            // This is a request using an absolute URL to the registered host
            // Relativize the path and use registered credentials
            sessionPoolKey = HTTPSessionPoolKey{fileURI.getHost(),
                fileURI.getPort(), false, fileURI.getScheme() == "https"};
            if (endpoint.getPath().empty())
                effectiveFileId = fileURI.getPathAndQuery();
            else {
                effectiveFileId =
                    fileURI.getPathAndQuery().substr(endpoint.getPath().size());
            }
        }
        else {
            // This is a request to an external resource on an external
            // server, with respect to the registered server.
            sessionPoolKey = HTTPSessionPoolKey{fileURI.getHost(),
                fileURI.getPort(), true, fileURI.getScheme() == "https"};
            effectiveFileId = fileURI.getPathAndQuery();
        }
    }

    res.first = std::move(sessionPoolKey);
    res.second = std::move(effectiveFileId);

    return res;
}

folly::Future<folly::Unit> HTTPHelper::checkStorageAvailability()
{
    LOG_FCALL();

    return options();
}

folly::Future<folly::Unit> HTTPHelper::options()
{
    return options(kHTTPRetryCount);
}

folly::Future<folly::Unit> HTTPHelper::options(
    const int retryCount, const Poco::URI &redirectURL)
{
    HTTPSessionPoolKey sessionPoolKey{};
    folly::fbstring effectiveFileId{};

    // Try to parse the fileId as URL - if it contains Host - treat it
    // as an external resource and create a separate HTTP session pool key
    auto poolKeyAndUri = relativizeURI("/");
    sessionPoolKey = std::move(poolKeyAndUri.first);
    effectiveFileId = std::move(poolKeyAndUri.second);

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey = HTTPSessionPoolKey{redirectURL.getHost(),
            redirectURL.getPort(), false, redirectURL.getScheme() == "https"};
    }

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.http.getattr");

    return connect(sessionPoolKey)
        .thenValue([fileId = effectiveFileId, timer = std::move(timer),
                       retryCount,
                       s = std::weak_ptr<HTTPHelper>{shared_from_this()}](
                       HTTPSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<HTTPOPTIONS>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)()
                .thenValue(
                    [&nsMap = self->m_nsMap, request,
                        fileMode = self->P()->fileMode()](
                        std::map<folly::fbstring, folly::fbstring>
                            && /*headers*/) { return folly::makeFuture(); })
                .thenError(folly::tag_t<HTTPFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting HTTP getattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->options(
                            retryCount - 1, Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.http.getattr.retries")
                            LOG_DBG(1) << "Retrying HTTP getattr request for "
                                       << fileId << " due to " << e.what();
                            return folly::makeFuture()
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->options(retryCount - 1);
                                });
                        }

                        LOG_DBG(1) << "Failed HTTP getattr request for "
                                   << fileId << " due to " << e.what();
                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(
                    folly::tag_t<proxygen::HTTPException>{}, [=](auto &&e) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.http.getattr.retries")
                            LOG_DBG(1) << "Retrying HTTP getattr request for "
                                       << fileId << " due to " << e.what();
                            return folly::makeFuture()
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->options(retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<folly::Unit> HTTPHelper::access(
    const folly::fbstring &fileId, const int /*mask*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    return getattr(fileId, kHTTPRetryCount, {}).thenValue([](auto && /*stat*/) {
        return folly::Unit();
    });
}

folly::Future<struct stat> HTTPHelper::getattr(const folly::fbstring &fileId)
{
    return getattr(fileId, kHTTPRetryCount);
}

folly::Future<struct stat> HTTPHelper::getattr(const folly::fbstring &fileId,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    HTTPSessionPoolKey sessionPoolKey{};
    folly::fbstring effectiveFileId{};

    // Try to parse the fileId as URL - if it contains Host - treat it
    // as an external resource and create a separate HTTP session pool key
    auto poolKeyAndUri = relativizeURI(fileId);
    sessionPoolKey = std::move(poolKeyAndUri.first);
    effectiveFileId = std::move(poolKeyAndUri.second);

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey = HTTPSessionPoolKey{redirectURL.getHost(),
            redirectURL.getPort(), false, redirectURL.getScheme() == "https"};
    }

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.http.getattr");

    return connect(sessionPoolKey)
        .thenValue([fileId = effectiveFileId, timer = std::move(timer),
                       retryCount,
                       s = std::weak_ptr<HTTPHelper>{shared_from_this()}](
                       HTTPSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<struct stat>(ECANCELED);

            auto request = std::make_shared<HTTPHEAD>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)(fileId)
                .thenValue(
                    [&nsMap = self->m_nsMap, fileId, request,
                        fileMode = self->P()->fileMode()](
                        std::map<folly::fbstring, folly::fbstring> &&headers) {
                        struct stat attrs {
                        };
                        attrs.st_mode = S_IFREG | fileMode;

                        if (headers.find("last-modified") != headers.end()) {
                            auto dateStr = headers["last-modified"];
                            int timeZoneDifferential = 0;
                            auto dateTime = Poco::DateTimeParser::parse(
                                Poco::DateTimeFormat::RFC1123_FORMAT,
                                dateStr.toStdString(), timeZoneDifferential);

                            attrs.st_atim.tv_sec = attrs.st_mtim.tv_sec =
                                attrs.st_ctim.tv_sec =
                                    dateTime.timestamp().epochTime();
                            attrs.st_atim.tv_nsec = attrs.st_mtim.tv_nsec =
                                attrs.st_ctim.tv_nsec = 0;
                        }

                        if (headers.find("content-length") != headers.end()) {
                            try {
                                attrs.st_size = std::stoll(
                                    headers["content-length"].toStdString());
                            }
                            catch (const std::invalid_argument &e) {
                                LOG(ERROR)
                                    << "Failed to parse resource content "
                                       "length: '"
                                    << headers["content-length"]
                                    << "' for resource: " << fileId;

                                attrs.st_size = 0;
                            }
                        }
                        return attrs;
                    })
                .thenError(folly::tag_t<HTTPFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting HTTP getattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->getattr(fileId, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.http.getattr.retries")
                            LOG_DBG(1) << "Retrying HTTP getattr request for "
                                       << fileId << " due to " << e.what();
                            return folly::makeFuture()
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->getattr(
                                        fileId, retryCount - 1);
                                });
                        }

                        LOG_DBG(1) << "Failed HTTP getattr request for "
                                   << fileId << " due to " << e.what();
                        return makeFuturePosixException<struct stat>(
                            e.code().value());
                    })
                .thenError(
                    folly::tag_t<proxygen::HTTPException>{}, [=](auto &&e) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.http.getattr.retries")
                            LOG_DBG(1) << "Retrying HTTP getattr request for "
                                       << fileId << " due to " << e.what();
                            return folly::makeFuture()
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->getattr(
                                        fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<struct stat>(EIO);
                    });
        });
}

folly::Future<HTTPSession *> HTTPHelper::connect(HTTPSessionPoolKey key)
{
    LOG_FCALL();

    if (!isAccessTokenValid())
        return makeFuturePosixException<HTTPSession *>(EKEYEXPIRED);

    if (P()->testTokenRefreshMode())
        return folly::makeFuture<HTTPSession *>(nullptr);

    if (std::get<0>(key).empty())
        key = HTTPSessionPoolKey{P()->endpoint().getHost(),
            P()->endpoint().getPort(), false,
            P()->endpoint().getScheme() == "https"};

    initializeSessionPool(key);

    // Wait for a http session to be available
    HTTPSession *httpSession{nullptr};
    decltype(m_idleSessionPool)::accessor ispAcc;
    m_idleSessionPool.find(ispAcc, key);
    auto idleSessionAvailable = ispAcc->second.try_pop(httpSession);

    if (!idleSessionAvailable) {
        LOG(ERROR)
            << "HTTP idle session connection pool empty - delaying request by "
               "10ms. In case this message shows frequently, consider "
               "increasing connectionPoolSize for the given storage.";
        const auto kHTTPIdleSessionWaitDelay = 10UL;
        return folly::makeFuture()
            .delayed(std::chrono::milliseconds(kHTTPIdleSessionWaitDelay))
            .thenValue([this, key](auto && /*unit*/) { return connect(key); });
    }

    assert(httpSession != nullptr);

    // Assign an EventBase to the session if it hasn't been assigned yet
    if (httpSession->evb == nullptr)
        httpSession->evb = m_executor->getEventBase();

    if (!httpSession->connectionPromise) {
        httpSession->reset();
    }

    return folly::via(httpSession->evb,
        [this, evb = httpSession->evb, httpSession,
            s = std::weak_ptr<HTTPHelper>{shared_from_this()}]() mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<HTTPSession *>(ECANCELED);

            auto p = self->params().get();

            if (!httpSession->closedByRemote && httpSession->sessionValid &&
                (httpSession->session != nullptr) &&
                !httpSession->session->isClosing()) {
                // NOLINTNEXTLINE
                return folly::via(evb, [httpSession]() { return httpSession; });
            }

            // Create a thread local timer for timeouts
            if (!m_sessionContext->timer) {
                m_sessionContext->timer = folly::HHWheelTimer::newTimer(evb,
                    std::chrono::milliseconds(
                        folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
                    folly::AsyncTimeout::InternalEnum::NORMAL, P()->timeout());
            }

            // Create a connector instance for the http session object
            if (!httpSession->connector) {
                httpSession->connector =
                    std::make_unique<proxygen::HTTPConnector>(
                        httpSession, m_sessionContext->timer.get());
            }

            // Check if we are already connecting on this thread
            if (!httpSession->connector->isBusy()) {
                httpSession->reset();

                auto host = std::get<0>(httpSession->key);
                auto port = std::get<1>(httpSession->key);
                auto isSecure = std::get<3>(httpSession->key);

                if (httpSession->address.empty())
                    httpSession->address =
                        folly::SocketAddress{host.toStdString(), port, true};

                LOG_DBG(2) << "Connecting to " << host << ":" << port;

                static const folly::SocketOptionMap socketOptions{
                    {{SOL_SOCKET, SO_REUSEADDR}, 1},
                    {{SOL_SOCKET, SO_KEEPALIVE}, 1}};

                if (isSecure) {
                    auto sslContext = std::make_shared<folly::SSLContext>(
                        folly::SSLContext::TLSv1_2);

                    if (!P()->verifyServerCertificate()) {
                        sslContext->authenticate(false, false);

                        folly::ssl::setSignatureAlgorithms<
                            folly::ssl::SSLCommonOptions>(*sslContext);
                    }

                    sslContext->setVerificationOption(
                        P()->verifyServerCertificate()
                            ? folly::SSLContext::SSLVerifyPeerEnum::VERIFY
                            : folly::SSLContext::SSLVerifyPeerEnum::NO_VERIFY);

                    auto *sslCtx = sslContext->getSSLCtx();
#if (OPENSSL_VERSION_NUMBER >= 0x10100000L)
                    SSL_CTX_set_max_proto_version(sslCtx, TLS1_2_VERSION);
#endif
                    if (!setupOpenSSLCABundlePath(sslCtx)) {
                        SSL_CTX_set_default_verify_paths(sslCtx);
                    }

                    // NOLINTNEXTLINE
                    SSL_CTX_set_session_cache_mode(sslCtx,
                        SSL_CTX_get_session_cache_mode(sslCtx) |
                            SSL_SESS_CACHE_CLIENT);

                    httpSession->connector->connectSSL(evb,
                        httpSession->address, sslContext, nullptr,
                        P()->timeout(), socketOptions,
                        folly::AsyncSocket::anyAddress(), host.toStdString());
                }
                else {
                    httpSession->connector->connect(evb, httpSession->address,
                        P()->timeout(), socketOptions);
                }
            }

            return httpSession->connectionPromise->getFuture()
                .via(evb)
                .thenValue([httpSession](auto && /*unit*/) mutable {
                    // NOLINTNEXTLINE
                    return folly::makeFuture<HTTPSession *>(
                        std::move(httpSession)); // NOLINT
                });
        });
}

bool HTTPHelper::setupOpenSSLCABundlePath(SSL_CTX *ctx)
{
    std::deque<std::string> caBundlePossibleLocations{
        "/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/pki/tls/certs/ca-bundle.trust.crt",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"};

    if (auto *sslCertFileEnv = std::getenv("SSL_CERT_FILE")) {
        caBundlePossibleLocations.push_front(sslCertFileEnv);
    }

    auto it = std::find_if(caBundlePossibleLocations.begin(),
        caBundlePossibleLocations.end(), [](const auto &path) {
            namespace bf = boost::filesystem;
            return bf::exists(path) &&
                (bf::is_regular_file(path) || bf::is_symlink(path));
        });

    if (it != caBundlePossibleLocations.end()) {
        if (SSL_CTX_load_verify_locations(ctx, it->c_str(), nullptr) == 0) {
            LOG(ERROR) << "Invalid CA bundle at " << *it
                       << ". Certificate server verification may not work "
                          "properly...";
            return false;
        }

        return true;
    }

    return false;
}

void HTTPHelper::addCookie(const std::string &host, const std::string &cookie)
{
    LOG_FCALL() << LOG_FARG(host) << LOG_FARG(cookie);

    decltype(m_cookies)::accessor cAcc;
    m_cookies.insert(cAcc, host);
    cAcc->second.push_back(cookie);
}

void HTTPHelper::clearCookies(const std::string &host)
{
    LOG_FCALL() << LOG_FARG(host);

    decltype(m_cookies)::accessor cAcc;
    m_cookies.erase(host);
}

std::vector<std::string> HTTPHelper::cookies(const std::string &host) const
{
    LOG_FCALL() << LOG_FARG(host);

    decltype(m_cookies)::accessor cAcc;
    if (m_cookies.find(cAcc, host))
        return cAcc->second;

    return {};
}

void HTTPSession::connectSuccess(
    proxygen::HTTPUpstreamSession *reconnectedSession)
{
    assert(reconnectedSession != nullptr);
    assert(reconnectedSession->getEventBase() == this->evb);

    reconnectedSession->setInfoCallback(this);

    LOG_DBG(2) << "New connection created with session " << reconnectedSession;

    if (session != nullptr) {
        LOG_DBG(4) << "Shutting down session transport";
        session->closeWhenIdle();
    }

    session = reconnectedSession;
    reconnectedSession->setMaxConcurrentIncomingStreams(1);
    reconnectedSession->setMaxConcurrentOutgoingStreams(1);
    sessionValid = true;
    connectionPromise->setValue();

    ONE_METRIC_COUNTER_INC("comp.helpers.mod.http.connections.active")
}

void HTTPSession::connectError(const folly::AsyncSocketException &ex)
{
    LOG(ERROR) << "Error when connecting to " + helper->endpoint().toString() +
            ": " + ex.what();

    // Reset socket address in case the address resolution changed
    address = folly::SocketAddress{};

    helper->releaseSession(this);

    connectionPromise->setWith([]() { throw makePosixException(EAGAIN); });
}

/**
 * HTTPRequest base
 */
HTTPRequest::HTTPRequest(HTTPHelper *helper, HTTPSession *session)
    : m_helper{helper}
    , m_session{session}
    , m_txn{nullptr}
    , m_path{helper->endpoint().getPath()}
    , m_resultCode{}
    , m_resultBody{std::make_unique<folly::IOBufQueue>(
          folly::IOBufQueue::cacheChainLength())}
{
    auto p =
        std::dynamic_pointer_cast<HTTPHelperParams>(helper->params().get());

    auto isExternal = std::get<2>(session->key);

    m_request.setHTTPVersion(kHTTPVersionMajor, kHTTPVersionMinor);
    if (m_request.getHeaders().getNumberOfValues("User-Agent") == 0U) {
        m_request.getHeaders().add("User-Agent", "Onedata");
    }
    if (m_request.getHeaders().getNumberOfValues("Accept") == 0U) {
        m_request.getHeaders().add("Accept", "*/*");
    }
    if (m_request.getHeaders().getNumberOfValues("Connection") == 0U) {
        if ((p->maxRequestsPerSession() > 0U) &&
            (m_session->session->getNumTxnServed() >=
                p->maxRequestsPerSession()))
            m_request.getHeaders().add("Connection", "Close");
        else
            m_request.getHeaders().add("Connection", "Keep-Alive");
    }
    if (m_request.getHeaders().getNumberOfValues("Host") == 0U) {
        m_request.getHeaders().add(
            "Host", m_helper->hostHeader().toStdString());
    }
    if (m_request.getHeaders().getNumberOfValues("Authorization") == 0U &&
        !isExternal) {
        if (p->credentialsType() == HTTPCredentialsType::NONE) { }
        else if (p->credentialsType() == HTTPCredentialsType::BASIC) {
            std::stringstream b64Stream;
            Poco::Base64Encoder b64Encoder(b64Stream);
            b64Encoder << p->credentials();
            b64Encoder.close();
            m_request.getHeaders().add(
                "Authorization", folly::sformat("Basic {}", b64Stream.str()));
        }
        else if (p->credentialsType() == HTTPCredentialsType::TOKEN) {
            Poco::StringTokenizer authHeader(
                p->authorizationHeader().toStdString(), ":",
                Poco::StringTokenizer::TOK_IGNORE_EMPTY |
                    Poco::StringTokenizer::TOK_TRIM);

            if (authHeader.count() == 1)
                m_request.getHeaders().add(
                    folly::sformat("{}", folly::trimWhitespace(authHeader[0])),
                    p->credentials().toStdString());
            else if (authHeader.count() == 2) {
                m_request.getHeaders().add(folly::sformat("{}", authHeader[0]),
                    folly::sformat(authHeader[1], p->credentials()));
            }
            else {
                LOG(WARNING) << "Unexpected token authorization header value: "
                             << p->authorizationHeader();
            }
        }
        else if (p->credentialsType() == HTTPCredentialsType::OAUTH2) {
            std::string b64BasicAuthorization;
            std::stringstream b64Stream;
            Poco::Base64Encoder b64Encoder(b64Stream);
            b64Encoder << p->credentials() << ":" << p->accessToken();
            b64Encoder.close();
            b64BasicAuthorization = b64Stream.str();
            m_request.getHeaders().add("Authorization",
                folly::sformat("Basic {}", b64BasicAuthorization));
        }
    }

    // Add cookies if any were stored in the cookie jar for this host
    const auto host = std::get<0>(session->key).toStdString();
    const auto cookies = m_helper->cookies(host);
    for (const auto &cookie : cookies) {
        m_request.getHeaders().add("Cookie", cookie);
    }
}

folly::Future<proxygen::HTTPTransaction *> HTTPRequest::startTransaction()
{
    assert(eventBase() != nullptr);

    return folly::via(eventBase(), [this]() {
        auto *session = m_session->session;

        if (m_session->closedByRemote || !m_session->sessionValid ||
            session == nullptr || session->isClosing()) {
            LOG_DBG(2) << "HTTP Session " << session
                       << " invalid - creating new session...";
            m_session->reset();
            m_helper->releaseSession(std::move(m_session)); // NOLINT
            throw makePosixException(EAGAIN);
        }
        auto maxOutcomingStreams = session->getMaxConcurrentOutgoingStreams();
        auto maxHistOutcomingStreams =
            session->getHistoricalMaxOutgoingStreams();
        auto outcomingStreams = session->getNumOutgoingStreams();
        auto processedTransactions = session->getNumTxnServed();

        LOG_DBG(3) << "Session (" << session
                   << ") stats: " << maxHistOutcomingStreams << ", "
                   << maxOutcomingStreams << ", " << outcomingStreams << ", "
                   << processedTransactions << "\n";

        auto *txn = session->newTransaction(this);
        if (txn == nullptr) {
            m_helper->releaseSession(std::move(m_session)); // NOLINT
            throw makePosixException(EAGAIN);
        }
        return txn;
    });
}

void HTTPRequest::setTransaction(proxygen::HTTPTransaction *txn) noexcept
{
    assert(txn != nullptr);

    m_txn = txn;
}

void HTTPRequest::detachTransaction() noexcept
{
    try {
        if (m_session != nullptr) {
            m_helper->releaseSession(std::move(m_session)); // NOLINT
            m_session = nullptr;
        }
        m_destructionGuard.reset();
    }
    catch (...) {
    }
}

void HTTPRequest::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept
{
    try {
        if (VLOG_IS_ON(4)) {
            LOG_DBG(4) << "Got headers:";
            msg->getHeaders().forEach(
                [](const std::string &h, const std::string &v) {
                    LOG_DBG(4) << "\t " << h << " : " << v;
                });
        }

        if (msg->getHeaders().getNumberOfValues("Connection") != 0U) {
            if (msg->getHeaders().rawGet("Connection") == "close") {
                LOG_DBG(4) << "Received 'Connection: close'";
                m_session->closedByRemote = true;
            }
        }
        if (msg->getHeaders().getNumberOfValues("Location") != 0U) {
            LOG_DBG(2) << "Received 302 redirect response to: "
                       << msg->getHeaders().rawGet("Location");
            m_redirectURL = Poco::URI(msg->getHeaders().rawGet("Location"));
            // Remember all cookies used for redirect to the host
            // in location
            if (msg->getHeaders().getNumberOfValues("set-cookie") > 0U) {
                auto redirectHost =
                    Poco::URI(msg->getHeaders().rawGet("location")).getHost();

                m_helper->clearCookies(redirectHost);

                msg->getHeaders().forEachValueOfHeader("set-cookie",
                    [redirectHost = std::move(redirectHost), helper = m_helper](
                        const std::string &cookie) {
                        helper->addCookie(redirectHost, cookie);
                        return false;
                    });
            }
        }
        m_resultCode = msg->getStatusCode();

        processHeaders(msg);
    }
    catch (...) {
    }
}

void HTTPRequest::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept { }

void HTTPRequest::onTrailers(
    std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept
{
}

void HTTPRequest::onEOM() noexcept { }

void HTTPRequest::onUpgrade(proxygen::UpgradeProtocol protocol) noexcept { }

void HTTPRequest::onEgressPaused() noexcept { }

void HTTPRequest::onEgressResumed() noexcept { }

void HTTPRequest::updateRequestURL(const folly::fbstring &resource)
{
    if (m_redirectURL.empty()) {
        m_request.rawSetURL(
            ensureHttpPath(folly::sformat("{}/{}", m_path, resource)));
    }
    else {
        if (m_redirectURL.getQuery().empty())
            m_request.rawSetURL(
                ensureHttpPath(folly::sformat("{}/{}", m_path, resource)));
        else
            m_request.rawSetURL(folly::sformat("{}?{}",
                ensureHttpPath(folly::sformat("{}/{}", m_path, resource)),
                m_redirectURL.getQuery()));
    }
}

/**
 * GET
 */
#ifndef __clang_analyzer__
folly::Future<folly::IOBufQueue> HTTPGET::operator()(
    const folly::fbstring &resource, const off_t offset, const size_t size)
{
    if (size == 0)
        return folly::via(m_session->evb, [] {
            return folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
        });

    m_request.setMethod("GET");

    updateRequestURL(resource);

    if (offset == 0 && size == 1) {
        m_firstByteRequest = true;
        m_request.getHeaders().add("Range", "bytes=0-1");
    }
    else {
        m_request.getHeaders().add(
            "Range", folly::sformat("bytes={}-{}", offset, offset + size - 1));
    }

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue(
        [self = shared_from_this()](proxygen::HTTPTransaction *txn) {
            txn->sendHeaders(self->m_request);
            txn->sendEOM();
            return self->m_resultPromise.getFuture();
        });
}
#endif

void HTTPGET::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept
{
    m_resultBody->append(std::move(chain));
}

void HTTPGET::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

void HTTPGET::onEOM() noexcept
{
    try {
        if (static_cast<HTTPStatus>(m_resultCode) == HTTPStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                HTTPFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            if (!m_firstByteRequest) {
                m_resultPromise.setValue(std::move(*m_resultBody));
            }
            else {
                auto str = m_resultBody->pop_front()->moveToFbString();
                auto iobufq =
                    folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());
                iobufq.append(str.c_str(), 1);
                m_resultPromise.setValue(std::move(iobufq));
            }
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

/**
 * OPTIONS
 */
folly::Future<std::map<folly::fbstring, folly::fbstring>>
HTTPOPTIONS::operator()()
{
    m_request.setMethod("OPTIONS");

    updateRequestURL("");

    m_destructionGuard = shared_from_this();

    return startTransaction()
        .via(eventBase())
        .thenValue([this](proxygen::HTTPTransaction *txn) {
            txn->sendHeaders(m_request);
            txn->sendEOM();
            return m_resultPromise.getFuture();
        });
}

/**
 * HEAD
 */
folly::Future<std::map<folly::fbstring, folly::fbstring>> HTTPHEAD::operator()(
    const folly::fbstring &resource)
{
    m_request.setMethod("HEAD");

    updateRequestURL(resource);

    m_destructionGuard = shared_from_this();

    return startTransaction()
        .via(eventBase())
        .thenValue([this](proxygen::HTTPTransaction *txn) {
            txn->sendHeaders(m_request);
            txn->sendEOM();
            return m_resultPromise.getFuture();
        });
}

void HTTPHEAD::processHeaders(
    const std::unique_ptr<proxygen::HTTPMessage> &msg) noexcept
{
    std::map<folly::fbstring, folly::fbstring> res{};

    try {
        if (static_cast<HTTPStatus>(m_resultCode) == HTTPStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                HTTPFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);

        if (result != 0) {
            m_resultPromise.setException(makePosixException(result));
        }
        else {
            if (msg->getHeaders().getNumberOfValues("content-type") != 0U) {
                res.emplace(
                    "content-type", msg->getHeaders().rawGet("content-type"));
            }
            if (msg->getHeaders().getNumberOfValues("last-modified") != 0U) {
                res.emplace(
                    "last-modified", msg->getHeaders().rawGet("last-modified"));
            }
            if (msg->getHeaders().getNumberOfValues("content-length") != 0U) {
                res.emplace("content-length",
                    msg->getHeaders().rawGet("content-length"));
            }

            m_resultPromise.setValue(std::move(res));
        }
    }
    catch (...) {
    }
}

void HTTPHEAD::onEOM() noexcept { }

void HTTPHEAD::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

void HTTPOPTIONS::processHeaders(
    const std::unique_ptr<proxygen::HTTPMessage> & /*msg*/) noexcept
{
    std::map<folly::fbstring, folly::fbstring> res{};

    try {
        if (static_cast<HTTPStatus>(m_resultCode) == HTTPStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                HTTPFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);

        if (result != 0) {
            m_resultPromise.setException(makePosixException(result));
        }
        else {
            m_resultPromise.setValue(std::move(res));
        }
    }
    catch (...) {
    }
}

void HTTPOPTIONS::onEOM() noexcept { }

void HTTPOPTIONS::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

} // namespace helpers
} // namespace one
