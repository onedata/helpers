/**
 * @file webDAVHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "webDAVHelper.h"
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
    const auto kWebDAVStatusDivider = 100;
    if (httpStatus / kWebDAVStatusDivider == 2)
        return 0;

    if (httpStatus / kWebDAVStatusDivider < 4)
        return EIO;

    switch (static_cast<WebDAVStatus>(httpStatus)) {
        case WebDAVStatus::BadRequest:
            return EBADMSG;
        case WebDAVStatus::Unauthorized:
        case WebDAVStatus::Forbidden:
            return EPERM;
        case WebDAVStatus::NotFound:
            return ENOENT;
        case WebDAVStatus::MethodNotAllowed:
            return ENOTSUP;
        case WebDAVStatus::NotAcceptable:
        case WebDAVStatus::ProxyAuthenticationRequired:
            return EACCES;
        case WebDAVStatus::RequestTimeout:
            return EAGAIN;
        case WebDAVStatus::Conflict:
            return EBADMSG;
        case WebDAVStatus::Gone:
            return ENXIO;
        case WebDAVStatus::LengthRequired:
        case WebDAVStatus::PreconditionFailed:
            return EINVAL;
        case WebDAVStatus::PayloadTooLarge:
            return EFBIG;
        case WebDAVStatus::URITooLong:
        case WebDAVStatus::UnsupportedMediaType:
            return EINVAL;
        case WebDAVStatus::RangeNotSatisfiable:
            return ERANGE;
        case WebDAVStatus::ExpectationFailed:
        case WebDAVStatus::UpgradeRequired:
        case WebDAVStatus::PreconditionRequired:
            return EINVAL;
        case WebDAVStatus::TooManyRequests:
            return EBUSY;
        case WebDAVStatus::RequestHeaderFieldsTooLarge:
            return EFBIG;
        case WebDAVStatus::UnavailableForLegalReasons:
            return EPERM;
        case WebDAVStatus::InternalServerError:
            return EIO;
        case WebDAVStatus::NotImplemented:
            return ENOTSUP;
        case WebDAVStatus::BadGateway:
        case WebDAVStatus::ServiceUnavailable:
            return ENXIO;
        case WebDAVStatus::GatewayTimeout:
            return EAGAIN;
        case WebDAVStatus::HTTPVersionNotSupported:
            return EINVAL;
        case WebDAVStatus::NetworkAuthenticationRequired:
            return EACCES;
        default:
            return EIO;
    }
}

// Retry only in case one of these errors occured
const std::set<int> &WebDAVRetryErrors()
{
    static const std::set<int> WebDAV_RETRY_ERRORS = {EINTR, EIO, EAGAIN,
        EACCES, EBUSY, EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK,
        EWOULDBLOCK, ENONET, ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN,
        ENETUNREACH, ECONNABORTED, ECONNRESET, ENOTCONN, EHOSTDOWN,
        EHOSTUNREACH, EREMOTEIO, ENOMEDIUM, ECANCELED};
    return WebDAV_RETRY_ERRORS;
}

inline bool shouldRetryError(int ec)
{
    return WebDAVRetryErrors().find(ec) != WebDAVRetryErrors().cend();
}

inline auto retryDelay(int retriesLeft)
{
    const unsigned int kWebDAVRetryBaseDelay_ms = 100;
    return kWebDAVRetryMinimumDelay +
        std::chrono::milliseconds{kWebDAVRetryBaseDelay_ms *
            (kWebDAVRetryCount - retriesLeft) *
            (kWebDAVRetryCount - retriesLeft)};
}

inline std::string ensureHttpPath(const folly::fbstring &path)
{
    if (path.empty())
        return "/";

    auto result = folly::trimWhitespace(path);

    if (result[0] != '/')
        return folly::sformat("/{}", result);

    return folly::sformat("{}", result);
}

inline std::string ensureCollectionPath(const folly::fbstring &path)
{
    auto result = ensureHttpPath(path);

    if (result.back() != '/')
        return result + '/';

    return result;
}

inline std::string buildRootPath(
    const std::string &endpointPath, const std::string &fileId)
{
    Poco::StringTokenizer endpointElements(endpointPath, "/",
        Poco::StringTokenizer::TOK_IGNORE_EMPTY |
            Poco::StringTokenizer::TOK_TRIM);

    Poco::StringTokenizer fileElements(fileId, "/",
        Poco::StringTokenizer::TOK_IGNORE_EMPTY |
            Poco::StringTokenizer::TOK_TRIM);

    std::vector<std::string> elements;

    elements.insert(std::end(elements), std::begin(endpointElements),
        std::end(endpointElements));
    elements.insert(
        std::end(elements), std::begin(fileElements), std::end(fileElements));

    return std::accumulate(elements.begin(), elements.end(), std::string(),
        [](const std::string &a, const std::string &b) -> std::string {
            return a + (a.length() > 0 ? "/" : "") + b;
        });
}
} // namespace

void WebDAVSession::reset()
{
    sessionValid = false;
    closedByRemote = false;
    session = nullptr;
    connectionPromise = std::make_unique<folly::SharedPromise<folly::Unit>>();
}

WebDAVFileHandle::WebDAVFileHandle(
    const folly::fbstring &fileId, std::shared_ptr<WebDAVHelper> helper)
    : FileHandle{fileId, std::move(helper)}
    , m_fileId{fileId}
{
    LOG_FCALL() << LOG_FARG(fileId);
}

folly::Future<folly::IOBufQueue> WebDAVFileHandle::read(
    const off_t offset, const std::size_t size)
{
    return read(offset, size, kWebDAVRetryCount);
}

folly::Future<folly::IOBufQueue> WebDAVFileHandle::read(const off_t offset,
    const std::size_t size, const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.read");

    auto helper = std::dynamic_pointer_cast<WebDAVHelper>(this->helper());

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return helper->connect(sessionPoolKey)
        .thenValue([fileId = m_fileId, redirectURL, offset, size, retryCount,
                       timer = std::move(timer), helper,
                       self = shared_from_this()](
                       WebDAVSession *session) mutable {
            auto getRequest =
                std::make_shared<WebDAVGET>(helper.get(), session);

            if (!redirectURL.empty())
                getRequest->setRedirectURL(redirectURL);

            return (*getRequest)(fileId, offset, size)
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, offset, size, retryCount](auto &&redirect) {
                        LOG_DBG(2) << "Redirecting WebDAV read request of file "
                                   << fileId << " to: " << redirect.location;
                        return self->read(offset, size, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [self, helper, evb = session->evb, offset, size,
                        retryCount](auto &&e) {
                        if (e.code().value() == ERANGE) {
                            return folly::makeFuture<folly::IOBufQueue>(
                                folly::IOBufQueue(
                                    folly::IOBufQueue::cacheChainLength()));
                        }

                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.read.retries");
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([self, offset, size, retryCount](
                                               auto /*unit*/) {
                                    return self->read(
                                        offset, size, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::IOBufQueue>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [self, helper, evb = session->evb, offset, size,
                        retryCount](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.read.retries");
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([self, offset, size, retryCount](
                                               auto && /*unit*/) {
                                    return self->read(
                                        offset, size, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::IOBufQueue>(EIO);
                    })
                .thenValue([timer = std::move(timer), getRequest, helper](
                               folly::IOBufQueue &&buf) {
                    ONE_METRIC_TIMERCTX_STOP(timer, buf.chainLength());
                    return std::move(buf);
                });
        });
}

folly::Future<std::size_t> WebDAVFileHandle::write(
    const off_t offset, folly::IOBufQueue buf, WriteCallback &&writeCb)
{
    return write(offset, std::move(buf), kWebDAVRetryCount)
        .thenValue([writeCb = std::move(writeCb)](std::size_t &&written) {
            if (writeCb)
                writeCb(written);
            return written;
        });
}

folly::Future<std::size_t> WebDAVFileHandle::write(const off_t offset,
    folly::IOBufQueue buf, const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.write");

    auto helper = std::dynamic_pointer_cast<WebDAVHelper>(this->helper());

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return helper->connect(sessionPoolKey)
        .thenValue([fileId = m_fileId, offset, buf = std::move(buf),
                       timer = std::move(timer), retryCount, redirectURL,
                       helper,
                       s = std::weak_ptr<WebDAVFileHandle>{shared_from_this()}](
                       WebDAVSession *session) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<std::size_t>(ECANCELED);

            std::shared_ptr<WebDAVRequest> request;

            if (helper->rangeWriteSupport() ==
                WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE) {
                request = std::make_shared<WebDAVPATCH>(helper.get(), session);
            }
            else if (helper->rangeWriteSupport() ==
                WebDAVRangeWriteSupport::MODDAV_PUTRANGE) {
                request = std::make_shared<WebDAVPUT>(helper.get(), session);
            }

            if (!redirectURL.empty())
                request->setRedirectURL(redirectURL);

            if (request) {
                auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();
                if (iobuf->isChained()) {
                    iobuf->unshare();
                    iobuf->coalesce();
                }

                auto size = iobuf->length();

                auto queue = std::make_shared<folly::IOBufQueue>(
                    folly::IOBufQueue::cacheChainLength());
                queue->append(iobuf->cloneOne());

                folly::Future<folly::Unit> req =
                    (helper->rangeWriteSupport() ==
                        WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE)
                    ? (*std::dynamic_pointer_cast<WebDAVPATCH>(request))(fileId,
                          offset, std::move(iobuf))
                    : (*std::dynamic_pointer_cast<WebDAVPUT>(request))(fileId,
                          offset, std::move(iobuf));

                return std::move(req)
                    .via(session->evb)
                    .thenValue([size, timer = std::move(timer), helper, request,
                                   fileId](auto && /*unit*/) {
                        ONE_METRIC_TIMERCTX_STOP(timer, size);
                        return size;
                    })
                    .thenError(folly::tag_t<WebDAVFoundException>{},
                        [fileId, self, offset, queue, retryCount](
                            auto &&redirect) {
                            LOG_DBG(2)
                                << "Redirecting WebDAV write request of file "
                                << fileId << " to: " << redirect.location;
                            return self->write(offset, std::move(*queue),
                                retryCount - 1, Poco::URI(redirect.location));
                        })
                    .thenError(folly::tag_t<std::system_error>{},
                        [self, evb = session->evb, offset, queue, retryCount](
                            auto &&e) mutable {
                            if (shouldRetryError(e.code().value()) &&
                                retryCount > 0) {

                                ONE_METRIC_COUNTER_INC(
                                    "comp.helpers.mod.webdav.write.retries");
                                return folly::makeSemiFuture()
                                    .via(evb)
                                    .delayed(retryDelay(retryCount))
                                    .thenValue([self, offset, retryCount,
                                                   queue = std::move(queue)](
                                                   auto && /*unit*/) {
                                        return self->write(offset,
                                            std::move(*queue), retryCount - 1);
                                    });
                            }
                            return makeFuturePosixException<std::size_t>(
                                e.code().value());
                        })
                    .thenError(folly::tag_t<proxygen::HTTPException>{},
                        [self, evb = session->evb, offset, queue, retryCount](
                            auto && /*unused*/) {
                            if (retryCount > 0) {

                                ONE_METRIC_COUNTER_INC(
                                    "comp.helpers.mod.webdav.write.retries");
                                return folly::makeSemiFuture()
                                    .via(evb)
                                    .delayed(retryDelay(retryCount))
                                    .thenValue([self, offset, retryCount,
                                                   queue](auto && /*unit*/) {
                                        return self->write(offset,
                                            std::move(*queue), retryCount - 1);
                                    });
                            }
                            return makeFuturePosixException<std::size_t>(EIO);
                        });
            }

            return makeFuturePosixException<std::size_t>(ENOTSUP);
        });
}

const Timeout &WebDAVFileHandle::timeout() { return helper()->timeout(); }

WebDAVHelper::WebDAVHelper(std::shared_ptr<WebDAVHelperParams> params,
    std::shared_ptr<folly::IOExecutor> executor,
    ExecutionContext executionContext)
    : StorageHelper{executionContext}
    , m_executor{std::move(executor)}
{
    m_nsMap.declarePrefix("d", kNSDAV);
    m_nsMap.declarePrefix("o", kNSOnedata);

    invalidateParams()->setValue(std::move(params));

    initializeSessionPool(WebDAVSessionPoolKey{
        P()->endpoint().getHost(), P()->endpoint().getPort()});
}

WebDAVHelper::~WebDAVHelper()
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

bool WebDAVHelper::isAccessTokenValid() const
{
    // Refresh the token as soon as the token will be valid for
    // less than 60 seconds
    constexpr auto kWebDAVAccessTokenMinimumTTL = 60;
    std::chrono::seconds webDAVAccessTokenMinimumTTL{
        kWebDAVAccessTokenMinimumTTL};

    if (P()->testTokenRefreshMode())
        webDAVAccessTokenMinimumTTL = std::chrono::seconds{0};

    if (P()->credentialsType() == WebDAVCredentialsType::OAUTH2) {
        LOG_DBG(3) << "Checking WebDAV access token ttl: "
                   << P()->accessTokenTTL().count() << " left: "
                   << P()->accessTokenTTL().count() -
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now() - P()->createdOn())
                    .count();

        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now() - P()->createdOn()) <
            P()->accessTokenTTL() - webDAVAccessTokenMinimumTTL;
    }

    return true;
}

folly::Future<FileHandlePtr> WebDAVHelper::open(const folly::fbstring &fileId,
    const int /*flags*/, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto handle =
        std::make_shared<WebDAVFileHandle>(fileId, shared_from_this());

    return folly::makeFuture(handle);
}

folly::Future<folly::Unit> WebDAVHelper::access(
    const folly::fbstring &fileId, const int /*mask*/)
{
    return access(fileId, {}, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::access(const folly::fbstring &fileId,
    const int /*mask*/, const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.access");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, timer = std::move(timer), retryCount,
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPFIND>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)(fileId, 0, propFilter)
                .via(session->evb)
                .thenValue([](PAPtr<pxml::Document> && /*multistatus*/) {
                    return folly::makeFuture();
                })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV access request of file "
                            << fileId << " to: " << redirect.location;
                        return self->access(fileId, {}, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.access.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->access(
                                        fileId, {}, retryCount - 1);
                                });
                        }

                        LOG_DBG(2)
                            << "Received access error " << e.code().value();
                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.access.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->access(
                                        fileId, {}, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<struct stat> WebDAVHelper::getattr(const folly::fbstring &fileId)
{
    return getattr(fileId, kWebDAVRetryCount);
}

folly::Future<struct stat> WebDAVHelper::getattr(const folly::fbstring &fileId,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.getattr");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, timer = std::move(timer), retryCount,
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<struct stat>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPFIND>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)(fileId, 0, propFilter)
                .via(session->evb)
                .thenValue([&nsMap = self->m_nsMap, fileId, request,
                               fileMode = self->P()->fileMode(),
                               dirMode = self->P()->dirMode()](
                               PAPtr<pxml::Document> &&multistatus) {
                    struct stat attrs {
                    };

                    auto *resourceType = multistatus->getNodeByPathNS(
                        "d:multistatus/d:response/d:propstat/d:prop/"
                        "d:resourcetype",
                        nsMap);

                    if (!resourceType->hasChildNodes()) {
                        // Regular file
                        attrs.st_mode = S_IFREG | fileMode;
                    }
                    else {
                        // Collection
                        attrs.st_mode = S_IFDIR | dirMode;
                    }

                    auto *getLastModified = multistatus->getNodeByPathNS(
                        "d:multistatus/d:response/d:propstat/d:prop/"
                        "d:getlastmodified",
                        nsMap);

                    if (getLastModified != nullptr) {
                        int timeZoneDifferential = 0;
                        auto dateStr = getLastModified->innerText();
                        auto dateTime = Poco::DateTimeParser::parse(
                            Poco::DateTimeFormat::RFC1123_FORMAT, dateStr,
                            timeZoneDifferential);

                        attrs.st_atim.tv_sec = attrs.st_mtim.tv_sec =
                            attrs.st_ctim.tv_sec =
                                dateTime.timestamp().epochTime();
                        attrs.st_atim.tv_nsec = attrs.st_mtim.tv_nsec =
                            attrs.st_ctim.tv_nsec = 0;
                    }

                    auto *getContentLength = multistatus->getNodeByPathNS(
                        "d:multistatus/d:response/d:propstat/d:prop/"
                        "d:getcontentlength",
                        nsMap);

                    if ((getContentLength != nullptr) &&
                        !getContentLength->innerText().empty()) {
                        try {
                            attrs.st_size =
                                std::stoi(getContentLength->innerText());
                        }
                        catch (const std::invalid_argument &e) {
                            LOG(ERROR) << "Failed to parse resource content "
                                          "length: '"
                                       << getContentLength->innerText()
                                       << "' for resource: " << fileId;

                            attrs.st_size = 0;
                        }
                    }
                    else {
                        attrs.st_size = 0;
                    }

                    return attrs;
                })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV getattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->getattr(fileId, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.getattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->getattr(
                                        fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<struct stat>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.getattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
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

folly::Future<folly::Unit> WebDAVHelper::unlink(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    return unlink(fileId, {}, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::unlink(const folly::fbstring &fileId,
    const size_t /*currentSize*/, const int retryCount,
    const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.unlink");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<WebDAVDELETE>(self.get(), session);

            return (*request)(fileId)
                .via(session->evb)
                .thenValue(
                    [request](auto && /*unit*/) { return folly::makeFuture(); })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV unlink request of file "
                            << fileId << " to: " << redirect.location;
                        return self->unlink(fileId, {}, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.unlink.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->unlink(fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.unlink.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->unlink(fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<folly::Unit> WebDAVHelper::rmdir(const folly::fbstring &fileId)
{
    return rmdir(fileId, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::rmdir(const folly::fbstring &fileId,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.rmdir");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<WebDAVDELETE>(self.get(), session);

            return (*request)(fileId)
                .via(session->evb)
                .thenValue(
                    [request](auto && /*unit*/) { return folly::makeFuture(); })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV rmdir request of file "
                            << fileId << " to: " << redirect.location;
                        return self->rmdir(fileId, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.rmdir.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->rmdir(fileId, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.rmdir.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->rmdir(fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<folly::Unit> WebDAVHelper::truncate(
    const folly::fbstring &fileId, off_t size, const size_t currentSize)
{
    return truncate(fileId, size, currentSize, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::truncate(const folly::fbstring &fileId,
    off_t size, const size_t currentSize, const int retryCount,
    const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.truncate");

    if (size > 0) {
        if (static_cast<size_t>(size) == currentSize)
            return folly::makeFuture();

        if (static_cast<size_t>(size) < currentSize)
            return makeFuturePosixException(ERANGE);
    }

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, retryCount, timer = std::move(timer), size,
                       currentSize,
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<WebDAVPUT>(self.get(), session);

            if (size == 0) {
                return (*request)(fileId, size, folly::IOBuf::create(0))
                    .via(session->evb)
                    .thenValue([request](auto && /*unit*/) {
                        return folly::makeFuture();
                    });
            }

            auto fillBuf = folly::IOBuf::create(size - currentSize);
            for (auto i = 0ul; i < static_cast<size_t>(size) - currentSize;
                 i++) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                fillBuf->writableData()[i] = 0;
            }

            return (*request)(fileId, size, std::move(fillBuf))
                .via(session->evb)
                .thenValue(
                    [request](auto && /*unit*/) { return folly::makeFuture(); })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, size, currentSize, self, retryCount](
                        auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV truncate request of file "
                            << fileId << " to: " << redirect.location;
                        return self->truncate(fileId, size, currentSize,
                            retryCount - 1, Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.truncate.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->truncate(fileId, size,
                                        currentSize, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.truncate.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->truncate(fileId, size,
                                        currentSize, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<folly::Unit> WebDAVHelper::mknod(const folly::fbstring &fileId,
    const mode_t /*mode*/, const FlagsSet & /*flags*/, const dev_t /*rdev*/)
{
    return mknod(fileId, {}, {}, {}, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::mknod(const folly::fbstring &fileId,
    const mode_t /*mode*/, const FlagsSet & /*flags*/, const dev_t /*rdev*/,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.mknod");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<WebDAVPUT>(self.get(), session);

            return (*request)(fileId, 0, std::make_unique<folly::IOBuf>())
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV mknod request of file "
                            << fileId << " to: " << redirect.location;
                        return self->mknod(fileId, {}, {}, {}, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [self, evb = session->evb, fileId, retryCount](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            return self->mknod(fileId, {}, {}, {});
                        }

                        if (e.code().value() == EPERM && retryCount > 0) {
                            LOG_DBG(2)
                                << "Received EPERM when creating file "
                                << fileId
                                << " - could be missing parent directory...";

                            // EPERM during mknod can be returned in case some
                            // directory on the path doesn't exist, if this is
                            // the case, return ENOENT instead
                            Poco::StringTokenizer pathElements(
                                fileId.toStdString(), "/",
                                Poco::StringTokenizer::TOK_IGNORE_EMPTY |
                                    Poco::StringTokenizer::TOK_TRIM);
                            folly::fbstring tempPath = "/";
                            std::vector<folly::Future<struct stat>> futs;
                            for (auto i = 0ul; i < pathElements.count() - 1;
                                 i++) {
                                if (pathElements[i].empty())
                                    continue;
                                tempPath += pathElements[i] + "/";
                                futs.emplace_back(self->getattr(tempPath));
                            }
                            return folly::collectAll(futs).via(evb).thenValue(
                                [](std::vector<folly::Try<struct stat>>
                                        &&tries) {
                                    for (const auto &t : tries) {
                                        if (t.hasException())
                                            return makeFuturePosixException<
                                                folly::Unit>(ENOENT);
                                    }

                                    return makeFuturePosixException<
                                        folly::Unit>(EPERM);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .via(session->evb)
                .thenValue([request](auto && /*unit*/) {
                    return folly::makeFuture();
                });
        });
}

folly::Future<folly::Unit> WebDAVHelper::mkdir(
    const folly::fbstring &fileId, const mode_t /*mode*/)
{
    return mkdir(fileId, {}, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::mkdir(const folly::fbstring &fileId,
    const mode_t /*mode*/, const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    if (fileId.empty() || (fileId == "/"))
        return folly::makeFuture();

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.mkdir");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<WebDAVMKCOL>(self.get(), session);

            return (*request)(fileId)
                .via(session->evb)
                .thenValue(
                    [request](auto && /*unit*/) { return folly::makeFuture(); })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV mkdir request of file "
                            << fileId << " to: " << redirect.location;
                        return self->mkdir(fileId, {}, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (e.code().value() == ENOTSUP) {
                            // Some WebDAV implementations return
                            // MethodNotAllowed error in case the collection at
                            // given path already exists
                            return folly::makeFuture();
                        }

                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.mkdir.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->mkdir(fileId, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.mkdir.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->mkdir(fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<folly::Unit> WebDAVHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    return rename(from, to, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::rename(const folly::fbstring &from,
    const folly::fbstring &to, const int retryCount,
    const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.rename");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([from, to, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request = std::make_shared<WebDAVMOVE>(self.get(), session);

            return (*request)(from, to)
                .via(session->evb)
                .thenValue(
                    [request](auto && /*unit*/) { return folly::makeFuture(); })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [from, to, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV rename request of file "
                            << from << " to: " << redirect.location;
                        return self->rename(from, to, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.rename.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->rename(
                                        from, to, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<folly::Unit>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.rename.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->rename(
                                        from, to, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::Unit>(EIO);
                    });
        });
}

folly::Future<folly::fbvector<folly::fbstring>> WebDAVHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    return readdir(fileId, offset, count, kWebDAVRetryCount);
}

folly::Future<folly::fbvector<folly::fbstring>> WebDAVHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.readdir");

    if (count == 0 || offset < 0)
        return folly::fbvector<folly::fbstring>{};

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, offset, count, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPFIND>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)(fileId, 1, propFilter)
                .via(session->evb)
                .thenValue([&nsMap = self->m_nsMap, offset, count,
                               endpointPath =
                                   std::string(self->P()->endpoint().getPath()),
                               fileId,
                               request](PAPtr<pxml::Document> &&multistatus) {
                    try {
                        PAPtr<pxml::NodeList> responses =
                            multistatus->getElementsByTagNameNS(
                                kNSDAV, "response");

                        folly::fbvector<folly::fbstring> result;

                        Poco::StringTokenizer fileIdElements(
                            fileId.toStdString(), "/",
                            Poco::StringTokenizer::TOK_IGNORE_EMPTY |
                                Poco::StringTokenizer::TOK_TRIM);

                        auto entryCount = responses->length();

                        if (static_cast<size_t>(offset) >= entryCount)
                            return result;

                        result.reserve(responses->length());

                        auto directoryPath = ensureCollectionPath(fileId);

                        auto rootPath =
                            buildRootPath(endpointPath, fileId.toStdString());

                        for (auto i = 0ul; i < entryCount; i++) {
                            auto *response = responses->item(i);
                            if (response == nullptr ||
                                !response->hasChildNodes())
                                continue;

                            nsMap.declarePrefix("d", kNSDAV);
                            auto *href =
                                response->getNodeByPathNS("d:href", nsMap);

                            if (href == nullptr)
                                continue;

                            Poco::StringTokenizer pathElements(
                                href->innerText(), "/",
                                Poco::StringTokenizer::TOK_IGNORE_EMPTY |
                                    Poco::StringTokenizer::TOK_TRIM);

                            Poco::StringTokenizer rootElements(rootPath, "/",
                                Poco::StringTokenizer::TOK_IGNORE_EMPTY |
                                    Poco::StringTokenizer::TOK_TRIM);

                            if (pathElements.count() > 0 &&
                                (rootElements.count() !=
                                    pathElements.count())) {
                                auto pa =
                                    pathElements[pathElements.count() - 1];
                                LOG_DBG(3)
                                    << "WebDAV::readdir: Added '" << pa
                                    << "' to response dir list. (rootPath "
                                       "= "
                                    << rootPath << ")";
                                result.emplace_back(std::move(pa));
                            }
                        }

                        std::sort(result.begin(), result.end());

                        if ((offset > 0) || (count < entryCount)) {
                            folly::fbvector<folly::fbstring> subResult;
                            std::copy_n(result.begin() + offset,
                                std::min<size_t>(count, result.size() - offset),
                                std::back_inserter(subResult));
                            return subResult;
                        }

                        return result;
                    }
                    catch (std::exception &e) {
                        LOG(ERROR) << "Invalid response from server when "
                                      "trying to "
                                      "read directory "
                                   << fileId << ": " << e.what();
                        throw makePosixException(EIO);
                    }
                })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, offset, count, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV readdir request of file "
                            << fileId << " to: " << redirect.location;
                        return self->readdir(fileId, offset, count,
                            retryCount - 1, Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.readdir.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->readdir(
                                        fileId, offset, count, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<
                            folly::fbvector<folly::fbstring>>(e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.readdir.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->readdir(
                                        fileId, offset, count, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<
                            folly::fbvector<folly::fbstring>>(EIO);
                    });
        });
}

folly::Future<folly::fbstring> WebDAVHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    return getxattr(fileId, name, kWebDAVRetryCount);
}

folly::Future<folly::fbstring> WebDAVHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.getxattr");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, name, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::fbstring>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPFIND>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)(fileId, 0, propFilter)
                .via(session->evb)
                .thenValue([&nsMap = self->m_nsMap, fileId, name, request](
                               PAPtr<pxml::Document> &&multistatus) {
                    std::string nameEncoded;
                    Poco::URI::encode(name.toStdString(), "", nameEncoded);
                    auto *attributeNode = multistatus->getNodeByPathNS(
                        folly::sformat(
                            "d:multistatus/d:response/d:propstat/d:prop/"
                            "o:{}",
                            nameEncoded),
                        nsMap);

                    folly::fbstring result;

                    if ((attributeNode != nullptr) &&
                        (attributeNode->childNodes()->length() == 1)) {
                        auto *attributeValueNode = attributeNode->firstChild();
                        if ((attributeValueNode != nullptr) &&
                            (attributeValueNode->nodeType() ==
                                pxml::Node::TEXT_NODE)) {
                            result = attributeNode->innerText();
                        }
                        else {
                            LOG(WARNING)
                                << "Unprocessable " << name
                                << " property value returned for " << fileId;
                        }
                    }
                    else {
                        throw makePosixException(ENODATA);
                    }

                    return result;
                })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, name, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV getxattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->getxattr(fileId, name, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.getxattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->getxattr(
                                        fileId, name, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<folly::fbstring>(
                            e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.getxattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->getxattr(
                                        fileId, name, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<folly::fbstring>(EIO);
                    });
        });
}

folly::Future<folly::Unit> WebDAVHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool /*create*/,
    bool /*replace*/)
{
    return setxattr(fileId, name, value, {}, {}, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool /*create*/,
    bool /*replace*/, const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name) << LOG_FARG(value);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.setxattr");

    if (P()->testTokenRefreshMode()) {
        return connect().thenValue([](auto && /*unit*/) {});
    }

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, name, value, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPPATCH>(self.get(), session);

            return (*request)(fileId, name, value, false)
                .via(session->evb)
                .thenValue([fileId, name, request](auto && /*unit*/) {})
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, name, value, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV setxattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->setxattr(fileId, name, value, {}, {},
                            retryCount - 1, Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.setxattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->setxattr(fileId, name, value,
                                        {}, {}, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException(e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.setxattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->setxattr(fileId, name, value,
                                        {}, {}, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException(EIO);
                    });
        });
}

folly::Future<folly::Unit> WebDAVHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    return removexattr(fileId, name, kWebDAVRetryCount);
}

folly::Future<folly::Unit> WebDAVHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name,
    const int retryCount, const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    auto timer =
        ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.removexattr");

    if (P()->testTokenRefreshMode()) {
        return connect().thenValue([](auto && /*unit*/) {});
    }

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, name, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<folly::Unit>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPPATCH>(self.get(), session);

            return (*request)(fileId, name, "", true)
                .via(session->evb)
                .thenValue([fileId, name, request](auto && /*unit*/) {})
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, name, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV removexattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->removexattr(fileId, name, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.removexatr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->removexattr(
                                        fileId, name, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException(e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.removexattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->removexattr(
                                        fileId, name, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException(EIO);
                    });
        });
}

folly::Future<folly::fbvector<folly::fbstring>> WebDAVHelper::listxattr(
    const folly::fbstring &fileId)
{
    return listxattr(fileId, kWebDAVRetryCount);
}

folly::Future<folly::fbvector<folly::fbstring>> WebDAVHelper::listxattr(
    const folly::fbstring &fileId, const int retryCount,
    const Poco::URI &redirectURL)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer =
        ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.listxattr");

    auto sessionPoolKey = WebDAVSessionPoolKey{};

    if (!redirectURL.getHost().empty()) {
        sessionPoolKey =
            WebDAVSessionPoolKey{redirectURL.getHost(), redirectURL.getPort()};
    }

    return connect(sessionPoolKey)
        .thenValue([fileId, retryCount, timer = std::move(timer),
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       WebDAVSession *session) {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<
                    folly::fbvector<folly::fbstring>>(ECANCELED);

            auto request =
                std::make_shared<WebDAVPROPFIND>(self.get(), session);
            folly::fbvector<folly::fbstring> propFilter;

            return (*request)(fileId, 0, propFilter)
                .via(session->evb)
                .thenValue([&nsMap = self->m_nsMap, fileId, request](
                               PAPtr<pxml::Document> &&multistatus) {
                    folly::fbvector<folly::fbstring> result;

                    auto *prop = multistatus->getNodeByPathNS(
                        "d:multistatus/d:response/d:propstat/d:prop", nsMap);

                    if (prop != nullptr) {
                        for (auto i = 0ul; i < prop->childNodes()->length();
                             i++) {
                            auto *property = prop->childNodes()->item(i);

                            // Filter out Onedata extended attributes
                            if (property->namespaceURI() == kNSOnedata) {
                                result.emplace_back(property->localName());
                            }
                        }
                    }

                    return result;
                })
                .thenError(folly::tag_t<WebDAVFoundException>{},
                    [fileId, self, retryCount](auto &&redirect) {
                        LOG_DBG(2)
                            << "Redirecting WebDAV listxattr request of file "
                            << fileId << " to: " << redirect.location;
                        return self->listxattr(fileId, retryCount - 1,
                            Poco::URI(redirect.location));
                    })
                .thenError(folly::tag_t<std::system_error>{},
                    [=, evb = session->evb](auto &&e) {
                        if (shouldRetryError(e.code().value()) &&
                            retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.listxattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->listxattr(
                                        fileId, retryCount - 1);
                                });
                        }
                        return makeFuturePosixException<
                            folly::fbvector<folly::fbstring>>(e.code().value());
                    })
                .thenError(folly::tag_t<proxygen::HTTPException>{},
                    [=, evb = session->evb](auto && /*unused*/) {
                        if (retryCount > 0) {
                            ONE_METRIC_COUNTER_INC(
                                "comp.helpers.mod.webdav.listxattr.retries")
                            return folly::makeSemiFuture()
                                .via(evb)
                                .delayed(retryDelay(retryCount))
                                .thenValue([=](auto && /*unit*/) {
                                    return self->listxattr(
                                        fileId, retryCount - 1);
                                });
                        }

                        return makeFuturePosixException<
                            folly::fbvector<folly::fbstring>>(EIO);
                    });
        });
}

folly::Future<WebDAVSession *> WebDAVHelper::connect(WebDAVSessionPoolKey key)
{
    LOG_FCALL();

    if (!isAccessTokenValid())
        return makeFuturePosixException<WebDAVSession *>(EKEYEXPIRED);

    if (P()->testTokenRefreshMode())
        return folly::makeFuture<WebDAVSession *>(nullptr);

    if (std::get<0>(key).empty())
        key = WebDAVSessionPoolKey{
            P()->endpoint().getHost(), P()->endpoint().getPort()};

    initializeSessionPool(key);

    // Wait for a webdav session to be available
    WebDAVSession *webDAVSession{nullptr};
    decltype(m_idleSessionPool)::accessor ispAcc;
    m_idleSessionPool.find(ispAcc, key);
    auto idleSessionAvailable = ispAcc->second.read(webDAVSession);

    if (!idleSessionAvailable) {
        LOG_DBG(1) << "WebDAV idle session connection pool empty - delaying "
                      "request by "
                      "10ms. In case this message shows frequently, consider "
                      "increasing connectionPoolSize for the given storage.";
        const auto kWebDAVIdleSessionWaitDelay = 10UL;
        return folly::makeSemiFuture()
            .via(webDAVSession->evb)
            .delayed(std::chrono::milliseconds(kWebDAVIdleSessionWaitDelay))
            .thenValue([this, key](auto && /*unit*/) { return connect(key); });
    }

    assert(webDAVSession != nullptr);

    // Assign an EventBase to the session if it hasn't been assigned yet
    if (webDAVSession->evb == nullptr)
        webDAVSession->evb = m_executor->getEventBase();

    if (!webDAVSession->connectionPromise) {
        webDAVSession->reset();
    }

    return folly::makeSemiFuture()
        .via(webDAVSession->evb)
        .thenValue([this, evb = webDAVSession->evb, webDAVSession,
                       s = std::weak_ptr<WebDAVHelper>{shared_from_this()}](
                       auto && /*unit*/) mutable {
            auto self = s.lock();
            if (!self)
                return makeFuturePosixException<WebDAVSession *>(ECANCELED);

            auto p = self->params().get();

            if (!webDAVSession->closedByRemote && webDAVSession->sessionValid &&
                (webDAVSession->session != nullptr) &&
                !webDAVSession->session->isClosing()) {
                // NOLINTNEXTLINE
                return folly::via(
                    evb, [webDAVSession]() { return webDAVSession; });
            }

            // Create a thead local timer for timeouts
            if (!m_sessionContext->timer) {
                m_sessionContext->timer = folly::HHWheelTimer::newTimer(evb,
                    std::chrono::milliseconds(
                        folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
                    folly::AsyncTimeout::InternalEnum::NORMAL, P()->timeout());
            }

            // Create a connector instance for the webdav session object
            if (!webDAVSession->connector) {
                webDAVSession->connector =
                    std::make_unique<proxygen::HTTPConnector>(
                        webDAVSession, m_sessionContext->timer.get());
            }

            // Check if we are already connecting on this thread
            if (!webDAVSession->connector->isBusy()) {
                webDAVSession->reset();

                auto host = std::get<0>(webDAVSession->key);
                auto port = std::get<1>(webDAVSession->key);
                auto scheme = P()->endpoint().getScheme();

                folly::SocketAddress address{host.toStdString(), port, true};

                LOG_DBG(2) << "Connecting to " << host << ":" << port;

                static const folly::SocketOptionMap socketOptions{
                    {{SOL_SOCKET, SO_REUSEADDR}, 1}};

                if (scheme == "https") {
                    auto sslContext = std::make_shared<folly::SSLContext>();

                    sslContext->authenticate(
                        P()->verifyServerCertificate(), false);

                    folly::ssl::setSignatureAlgorithms<
                        folly::ssl::SSLCommonOptions>(*sslContext);

                    sslContext->setVerificationOption(
                        P()->verifyServerCertificate()
                            ? folly::SSLContext::SSLVerifyPeerEnum::VERIFY
                            : folly::SSLContext::SSLVerifyPeerEnum::NO_VERIFY);

                    auto *sslCtx = sslContext->getSSLCtx();
                    if (!setupOpenSSLCABundlePath(sslCtx)) {
                        SSL_CTX_set_default_verify_paths(sslCtx);
                    }

                    // NOLINTNEXTLINE
                    SSL_CTX_set_session_cache_mode(sslCtx,
                        SSL_CTX_get_session_cache_mode(sslCtx) |
                            SSL_SESS_CACHE_CLIENT);

                    webDAVSession->connector->connectSSL(evb, address,
                        sslContext, nullptr, P()->timeout(), socketOptions,
                        folly::AsyncSocket::anyAddress(),
                        P()->endpoint().getHost());
                }
                else {
                    webDAVSession->connector->connect(
                        evb, address, P()->timeout(), socketOptions);
                }
            }

            return webDAVSession->connectionPromise->getFuture()
                .via(evb)
                .thenValue([webDAVSession](auto && /*unit*/) mutable {
                    return folly::makeFuture<WebDAVSession *>(
                        std::move(webDAVSession)); // NOLINT
                });
        });
}

bool WebDAVHelper::setupOpenSSLCABundlePath(SSL_CTX *ctx)
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

void WebDAVSession::connectSuccess(
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
    host = reconnectedSession->getPeerAddress().getHostStr();
    reconnectedSession->setMaxConcurrentIncomingStreams(1);
    reconnectedSession->setMaxConcurrentOutgoingStreams(1);
    sessionValid = true;
    connectionPromise->setValue();
}

void WebDAVSession::connectError(const folly::AsyncSocketException &ex)
{
    LOG(ERROR) << "Error when connecting to " + helper->endpoint().toString() +
            ": " + ex.what();

    helper->releaseSession(this);
}

/**
 * WebDAVRequest base
 */
WebDAVRequest::WebDAVRequest(WebDAVHelper *helper, WebDAVSession *session)
    : m_helper{helper}
    , m_session{session}
    , m_txn{nullptr}
    , m_path{helper->endpoint().getPath()}
    , m_resultCode{}
    , m_resultBody{std::make_unique<folly::IOBufQueue>(
          folly::IOBufQueue::cacheChainLength())}
{
    auto p =
        std::dynamic_pointer_cast<WebDAVHelperParams>(helper->params().get());

    m_request.setHTTPVersion(kWebDAVHTTPVersionMajor, kWebDAVHTTPVersionMinor);
    if (m_request.getHeaders().getNumberOfValues("User-Agent") == 0U) {
        m_request.getHeaders().add("User-Agent", "Onedata");
    }
    if (m_request.getHeaders().getNumberOfValues("Accept") == 0U) {
        m_request.getHeaders().add("Accept", "*/*");
    }
    if (m_request.getHeaders().getNumberOfValues("Connection") == 0U) {
        m_request.getHeaders().add("Connection", "Keep-Alive");
    }
    if (m_request.getHeaders().getNumberOfValues("Host") == 0U) {
        m_request.getHeaders().add("Host", session->host);
    }
    if (m_request.getHeaders().getNumberOfValues("Authorization") == 0U) {
        if (p->credentialsType() == WebDAVCredentialsType::NONE) { }
        else if (p->credentialsType() == WebDAVCredentialsType::BASIC) {
            std::stringstream b64Stream;
            Poco::Base64Encoder b64Encoder(b64Stream);
            b64Encoder << p->credentials();
            b64Encoder.close();
            m_request.getHeaders().add(
                "Authorization", folly::sformat("Basic {}", b64Stream.str()));
        }
        else if (p->credentialsType() == WebDAVCredentialsType::TOKEN) {
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
        else if (p->credentialsType() == WebDAVCredentialsType::OAUTH2) {
            std::string b64BasicAuthorization;
            std::stringstream b64Stream;
            Poco::Base64Encoder b64Encoder(b64Stream);
            b64Encoder << p->credentials() << ":" << p->accessToken();
            b64Encoder.close();
            b64BasicAuthorization = b64Stream.str();
            m_request.getHeaders().add("Authorization",
                folly::sformat("Basic {}", b64BasicAuthorization));
        }
        else {
            LOG(ERROR) << "Unknown credentials type";
        }
    }
}

folly::Future<proxygen::HTTPTransaction *> WebDAVRequest::startTransaction()
{
    assert(eventBase() != nullptr);

    return folly::via(eventBase(), [this]() {
        auto *session = m_session->session;

        if (m_session->closedByRemote || !m_session->sessionValid ||
            session == nullptr || session->isClosing()) {
            LOG_DBG(4) << "HTTP Session " << session
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

        LOG_DBG(4) << "Session (" << session
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

void WebDAVRequest::setTransaction(proxygen::HTTPTransaction *txn) noexcept
{
    assert(txn != nullptr);

    m_txn = txn;
}

void WebDAVRequest::detachTransaction() noexcept
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

void WebDAVRequest::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept
{
    try {
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
        }
        m_resultCode = msg->getStatusCode();
    }
    catch (...) {
    }
}

void WebDAVRequest::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept { }

void WebDAVRequest::onTrailers(
    std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept
{
}

void WebDAVRequest::onEOM() noexcept { }

void WebDAVRequest::onUpgrade(proxygen::UpgradeProtocol protocol) noexcept { }

void WebDAVRequest::onEgressPaused() noexcept { }

void WebDAVRequest::onEgressResumed() noexcept { }

void WebDAVRequest::updateRequestURL(const folly::fbstring &resource)
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
folly::Future<folly::IOBufQueue> WebDAVGET::operator()(
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

void WebDAVGET::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept
{
    try {
        m_resultBody->append(std::move(chain));
    }
    catch (...) {
    }
}

void WebDAVGET::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

void WebDAVGET::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
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
 * PUT - only for servers supporting Content-Range header
 */
folly::Future<folly::Unit> WebDAVPUT::operator()(
    const folly::fbstring &resource, const off_t offset,
    std::unique_ptr<folly::IOBuf> buf)
{
    m_request.setMethod("PUT");

    updateRequestURL(resource);

    if (buf->length() > 0) {
        m_request.getHeaders().add("Content-Range",
            folly::sformat(
                "bytes {}-{}/*", offset, offset + buf->length() - 1));
    }
    m_request.getHeaders().add(
        "Content-length", folly::sformat("{}", buf->length()));

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue(
        [this, buf = std::move(buf)](proxygen::HTTPTransaction *txn) mutable {
            txn->sendHeaders(m_request);
            txn->sendBody(std::move(buf));
            txn->sendEOM();
            return m_resultPromise.getFuture();
        });
}

void WebDAVPUT::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            m_resultPromise.setValue();
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVPUT::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * PATCH - only for servers supporting SabreDAV PATCH Partial Update
 */
folly::Future<folly::Unit> WebDAVPATCH::operator()(
    const folly::fbstring &resource, const off_t offset,
    std::unique_ptr<folly::IOBuf> buf)
{
    if (buf->length() == 0)
        return folly::makeFuture();

    m_request.setMethod("PATCH");

    updateRequestURL(resource);

    m_request.getHeaders().add("X-Update-Range",
        folly::sformat("bytes={}-{}", offset, offset + buf->length() - 1));
    m_request.getHeaders().add(
        "Content-type", "application/x-sabredav-partialupdate");
    m_request.getHeaders().add(
        "Content-length", folly::sformat("{}", buf->length()));

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue(
        [this, buf = std::move(buf)](proxygen::HTTPTransaction *txn) mutable {
            txn->sendHeaders(m_request);
            txn->sendBody(std::move(buf));
            txn->sendEOM();
            return m_resultPromise.getFuture();
        });
}

void WebDAVPATCH::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept
{
    WebDAVRequest::onHeadersComplete(std::move(msg));
}

void WebDAVPATCH::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            m_resultPromise.setValue();
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVPATCH::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * MKCOL
 */
folly::Future<folly::Unit> WebDAVMKCOL::operator()(
    const folly::fbstring &resource)
{
    m_request.setMethod("MKCOL");

    updateRequestURL(resource);

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVMKCOL::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            m_resultPromise.setValue();
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVMKCOL::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * PROPFIND
 */
folly::Future<PAPtr<pxml::Document>> WebDAVPROPFIND::operator()(
    const folly::fbstring &resource, const int depth,
    const folly::fbvector<folly::fbstring> & /*propFilter*/)
{
    m_request.setMethod("PROPFIND");

    updateRequestURL(resource);

    m_request.getHeaders().add(
        "Depth", (depth >= 0) ? std::to_string(depth) : "infinity");

    m_destructionGuard = shared_from_this();

    return startTransaction()
        .via(eventBase())
        .thenValue([this](proxygen::HTTPTransaction *txn) {
            txn->sendHeaders(m_request);
            txn->sendEOM();
            return m_resultPromise.getFuture();
        });
}

void WebDAVPROPFIND::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept
{
    WebDAVRequest::onHeadersComplete(std::move(msg));
}

void WebDAVPROPFIND::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept
{
    m_resultBody->append(std::move(chain));
}

void WebDAVPROPFIND::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);

        if (result == 0) {
            // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
            m_resultPromise.setWith([bufq = std::move(m_resultBody)]() {
                bufq->gather(bufq->chainLength());
                auto iobuf =
                    bufq->empty() ? folly::IOBuf::create(0) : bufq->move();
                if (iobuf->isChained()) {
                    iobuf->unshare();
                    iobuf->coalesce();
                }

                pxml::DOMParser parser;
                pxml::Document *xml = nullptr;
                try {
                    xml = parser.parseMemory(
                        reinterpret_cast<const char *>(iobuf->data()),
                        iobuf->length());
                }
                catch (std::exception &e) {
                    LOG(ERROR) << "Invalid XML PROPFIND response: " << e.what();
                    if (xml != nullptr)
                        xml->release();
                    throw makePosixException(EIO);
                }
                return PAPtr<pxml::Document>(xml);
            });
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVPROPFIND::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * PROPPATCH
 */
folly::Future<folly::Unit> WebDAVPROPPATCH::operator()(
    const folly::fbstring &resource, const folly::fbstring &property,
    const folly::fbstring &value, const bool remove)
{
    m_request.setMethod("PROPPATCH");

    updateRequestURL(resource);

    m_request.getHeaders().add(
        "Content-type", "application/x-www-form-urlencoded");

    PAPtr<pxml::Document> propertyUpdate = new pxml::Document;
    PAPtr<pxml::Element> root = propertyUpdate->createElement("propertyupdate");
    root->setAttribute("xmlns", kNSDAV);
    root->setAttribute("xmlns:o", kNSOnedata);
    if (!remove) {
        PAPtr<pxml::Element> set = propertyUpdate->createElement("set");
        PAPtr<pxml::Element> prop = propertyUpdate->createElement("prop");

        std::string propertyNameEncoded;
        Poco::URI::encode(property.toStdString(), "", propertyNameEncoded);

        PAPtr<pxml::Element> propertyNode =
            propertyUpdate->createElementNS(kNSOnedata, propertyNameEncoded);
        PAPtr<pxml::Node> valueNode;
        if (!value.empty()) {
            valueNode = propertyUpdate->createTextNode(value.toStdString());
        }
        else {
            // WebDAV does not allow to store properties with empty value,
            // so we have to make this fact explicit by storing a special
            // XML tag as the property value
            valueNode = propertyUpdate->createElementNS(kNSOnedata, "Null");
        }
        propertyNode->appendChild(valueNode);

        prop->appendChild(propertyNode);
        set->appendChild(prop);
        root->appendChild(set);
    }
    else {
        PAPtr<pxml::Element> removeNode =
            propertyUpdate->createElement("remove");
        PAPtr<pxml::Element> prop = propertyUpdate->createElement("prop");
        std::string propertyNameEncoded;
        Poco::URI::encode(property.toStdString(), "", propertyNameEncoded);
        PAPtr<pxml::Element> propertyNode =
            propertyUpdate->createElementNS("o:", propertyNameEncoded);
        prop->appendChild(propertyNode);
        removeNode->appendChild(prop);
        root->appendChild(removeNode);
    }
    propertyUpdate->appendChild(root);

    std::stringstream bodyStream;
    pxml::DOMWriter writer;
    Poco::UTF8Encoding utf8Enc;
    writer.setEncoding("UTF-8", utf8Enc);
    writer.setOptions(Poco::XML::XMLWriter::Options::WRITE_XML_DECLARATION |
        Poco::XML::XMLWriter::Options::CANONICAL_XML);
    writer.writeNode(bodyStream, propertyUpdate);
    auto propUpdateString = bodyStream.str();

    m_request.getHeaders().add(
        "Content-length", folly::sformat("{}", propUpdateString.length()));

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue(
        [this, body = std::move(propUpdateString)](
            proxygen::HTTPTransaction *txn) {
            txn->sendHeaders(m_request);
            txn->sendBody(folly::IOBuf::copyBuffer(body));
            txn->sendEOM();
            return m_resultPromise.getFuture();
        });
}

void WebDAVPROPPATCH::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept
{
    WebDAVRequest::onHeadersComplete(std::move(msg));
}

void WebDAVPROPPATCH::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept
{
    m_resultBody->append(std::move(chain));
}

void WebDAVPROPPATCH::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);

        if (result == 0) {
            m_resultPromise.setWith([bufq = std::move(m_resultBody)]() {
                bufq->gather(bufq->chainLength());
                auto iobuf =
                    bufq->empty() ? folly::IOBuf::create(0) : bufq->move();
                if (iobuf->isChained()) {
                    iobuf->unshare();
                    iobuf->coalesce();
                }

                pxml::NamespaceSupport nsMap;
                nsMap.declarePrefix("d", kNSDAV);
                nsMap.declarePrefix("o", kNSOnedata);

                try {
                    pxml::DOMParser m_parser;
                    PAPtr<pxml::Document> xml;
                    xml = m_parser.parseMemory(
                        reinterpret_cast<const char *>(iobuf->data()),
                        iobuf->length());

                    auto *status = xml->getNodeByPathNS(
                        "d:multistatus/d:response/d:propstat/d:status", nsMap);

                    if (status == nullptr) {
                        throw makePosixException(EINVAL);
                    }

                    if ((std::string("HTTP/1.1 200 OK") ==
                            status->innerText()) ||
                        (std::string("HTTP/1.1 204 No Content") ==
                            status->innerText())) {
                        return folly::Unit{};
                    }

                    throw makePosixException(EINVAL);
                }
                catch (std::exception &e) {
                    LOG(ERROR)
                        << "Cannot parse PROPPATCH response: " << e.what();
                    throw makePosixException(EIO);
                }
            });
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVPROPPATCH::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * DELETE
 */
folly::Future<folly::Unit> WebDAVDELETE::operator()(
    const folly::fbstring &resource)
{
    m_request.setMethod("DELETE");

    updateRequestURL(resource);

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVDELETE::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            m_resultPromise.setValue();
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVDELETE::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * MOVE
 */
folly::Future<folly::Unit> WebDAVMOVE::operator()(
    const folly::fbstring &resource, const folly::fbstring &destination)
{
    m_request.setMethod("MOVE");

    updateRequestURL(resource);

    m_request.getHeaders().add("Destination", destination.toStdString());

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVMOVE::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            m_resultPromise.setValue();
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVMOVE::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

/**
 * COPY
 */
folly::Future<folly::Unit> WebDAVCOPY::operator()(
    const folly::fbstring &resource, const folly::fbstring &destination)
{
    m_request.setMethod("COPY");

    updateRequestURL(resource);

    m_request.getHeaders().add("Destination", destination.toStdString());

    m_destructionGuard = shared_from_this();

    return startTransaction().thenValue([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVCOPY::onEOM() noexcept
{
    try {
        if (static_cast<WebDAVStatus>(m_resultCode) == WebDAVStatus::Found) {
            // The request is being redirected to another URL
            m_resultPromise.setException(
                WebDAVFoundException{m_redirectURL.toString()});
            return;
        }

        auto result = httpStatusToPosixError(m_resultCode);
        if (result == 0) {
            m_resultPromise.setValue();
        }
        else {
            m_resultPromise.setException(makePosixException(result));
        }
    }
    catch (...) {
    }
}

void WebDAVCOPY::onError(const proxygen::HTTPException &error) noexcept
{
    try {
        m_resultPromise.setException(error);
    }
    catch (...) {
    }
}

} // namespace helpers
} // namespace one
