/**
 * @file webDAVHelper.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "webDAVHelper.h"
#include "logging.h"
#include "monitoring/monitoring.h"

#include <Poco/DOM/NodeList.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeParser.h>
#include <Poco/SAX/NamespaceSupport.h>
#include <Poco/URI.h>
#include <Poco/UTF8Encoding.h>
#include <expat.h>
#include <folly/Format.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/HHWheelTimer.h>
#include <folly/io/async/SSLOptions.h>
#include <glog/stl_logging.h>
#include <openssl/ssl.h>

#include <functional>

namespace one {
namespace helpers {

/**
 * Convert HTTP Status Code to appropriate POSIX error
 */
static int httpStatusToPosixError(uint16_t httpStatus)
{
    const auto kHTTPStatusDivider = 100;
    if (httpStatus / kHTTPStatusDivider == 2)
        return 0;

    if (httpStatus / kHTTPStatusDivider < 4)
        return EIO;

    switch (httpStatus) {
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
            return EACCES;
        case HTTPStatus::ProxyAuthenticationRequired:
            return EACCES;
        case HTTPStatus::RequestTimeout:
            return EAGAIN;
        case HTTPStatus::Conflict:
            return EBADMSG;
        case HTTPStatus::Gone:
            return ENXIO;
        case HTTPStatus::LengthRequired:
            return EINVAL;
        case HTTPStatus::PreconditionFailed:
            return EINVAL;
        case HTTPStatus::PayloadTooLarge:
            return EFBIG;
        case HTTPStatus::URITooLong:
            return EINVAL;
        case HTTPStatus::UnsupportedMediaType:
            return EINVAL;
        case HTTPStatus::RangeNotSatisfiable:
            return ERANGE;
        case HTTPStatus::ExpectationFailed:
            return EINVAL;
        case HTTPStatus::UpgradeRequired:
            return EINVAL;
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
            return ENXIO;
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

inline bool WebDAVRetryCondition(int result, const std::string &operation)
{
    // Retry only in case one of these errors occured
    const static std::set<int> WebDAV_RETRY_ERRORS = {EINTR, EIO, EAGAIN,
        EACCES, EBUSY, EMFILE, ETXTBSY, ESPIPE, EMLINK, EPIPE, EDEADLK,
        EWOULDBLOCK, ENONET, ENOLINK, EADDRINUSE, EADDRNOTAVAIL, ENETDOWN,
        ENETUNREACH, ECONNABORTED, ECONNRESET, ENOTCONN, EHOSTDOWN,
        EHOSTUNREACH, EREMOTEIO, ENOMEDIUM, ECANCELED};

    auto ret = (WebDAV_RETRY_ERRORS.find(-result) == WebDAV_RETRY_ERRORS.end());

    if (!ret) {
        LOG(WARNING) << "Retrying WebDAV helper operation '" << operation
                     << "' due to error: " << result;
        ONE_METRIC_COUNTER_INC(
            "comp.helpers.mod.webdav." + operation + ".retries");
    }

    return ret;
}

static inline std::string ensureHttpPath(const folly::fbstring &path)
{
    if (path.empty())
        return "/";

    auto result = folly::trimWhitespace(path);

    if (result[0] != '/')
        return folly::sformat("/{}", result);

    return folly::sformat("{}", result);
}

static inline std::string ensureCollectionPath(const folly::fbstring &path)
{
    auto result = ensureHttpPath(path);

    if (result.back() != '/')
        return result + '/';

    return result;
}

WebDAVFileHandle::WebDAVFileHandle(
    folly::fbstring fileId, std::shared_ptr<WebDAVHelper> helper)
    : FileHandle{fileId}
    , m_helper{std::move(helper)}
    , m_fileId{fileId}
{
    LOG_FCALL() << LOG_FARG(fileId);
}

folly::Future<folly::IOBufQueue> WebDAVFileHandle::read(
    const off_t offset, const std::size_t size)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.read");

    return m_helper->connect().then([
        fileId = m_fileId, offset, size, timer = std::move(timer),
        helper = m_helper, self = shared_from_this()
    ](folly::EventBase * evb) mutable {
        auto getRequest = std::make_shared<WebDAVGET>(*helper, evb);

        return (*getRequest)(fileId, offset, size).then([
            timer = std::move(timer), getRequest, helper
        ](folly::IOBufQueue && buf) {
            ONE_METRIC_TIMERCTX_STOP(timer, buf.chainLength());
            return std::move(buf);
        });
    });
}

folly::Future<std::size_t> WebDAVFileHandle::write(
    const off_t offset, folly::IOBufQueue buf)
{
    LOG_FCALL() << LOG_FARG(offset) << LOG_FARG(buf.chainLength());

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.write");

    return m_helper->connect().then([
        fileId = m_fileId, offset, buf = std::move(buf),
        rangeWriteSupport = m_helper->rangeWriteSupport(),
        timer = std::move(timer), helper = m_helper,
        s = std::weak_ptr<WebDAVFileHandle>{shared_from_this()}
    ](folly::EventBase * evb) mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<std::size_t>(ECANCELED);

        if (rangeWriteSupport ==
            WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE) {
            auto patchRequest = std::make_shared<WebDAVPATCH>(*helper, evb);

            auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();
            if (iobuf->isChained()) {
                iobuf->unshare();
                iobuf->coalesce();
            }

            auto size = iobuf->length();

            return (*patchRequest)(fileId, offset, std::move(iobuf)).then([
                size, timer = std::move(timer), helper, patchRequest
            ]() {
                ONE_METRIC_TIMERCTX_STOP(timer, size);
                return size;
            });
        }

        if (rangeWriteSupport == WebDAVRangeWriteSupport::MODDAV_PUTRANGE) {
            auto putRequest = std::make_shared<WebDAVPUT>(*helper, evb);

            auto iobuf = buf.empty() ? folly::IOBuf::create(0) : buf.move();
            if (iobuf->isChained()) {
                iobuf->unshare();
                iobuf->coalesce();
            }

            auto size = iobuf->length();

            return (*putRequest)(fileId, offset, std::move(iobuf)).then([
                size, timer = std::move(timer), helper, putRequest
            ]() {
                ONE_METRIC_TIMERCTX_STOP(timer, size);
                return size;
            });
        }

        return makeFuturePosixException<std::size_t>(ENOTSUP);
    });
}

const Timeout &WebDAVFileHandle::timeout() { return m_helper->timeout(); }

WebDAVHelper::WebDAVHelper(Poco::URI endpoint, bool verifyServerCertificate,
    WebDAVCredentialsType credentialsType, folly::fbstring credentials,
    folly::fbstring authorizationHeader,
    WebDAVRangeWriteSupport rangeWriteSupport,
    std::shared_ptr<folly::IOExecutor> executor, Timeout timeout)
    : m_endpoint{endpoint}
    , m_verifyServerCertificate{verifyServerCertificate}
    , m_credentialsType{credentialsType}
    , m_credentials{std::move(credentials)}
    , m_authorizationHeader{std::move(authorizationHeader)}
    , m_rangeWriteSupport{rangeWriteSupport}
    , m_executor{std::move(executor)}
    , m_timeout{timeout}
{
    LOG_FCALL() << LOG_FARG(m_endpoint.toString()) << LOG_FARG(m_credentials)
                << LOG_FARG(m_authorizationHeader);

    m_nsMap.declarePrefix("d", kNSDAV);
    m_nsMap.declarePrefix("o", kNSOnedata);
}

WebDAVHelper::~WebDAVHelper() { LOG_FCALL(); }

folly::Future<FileHandlePtr> WebDAVHelper::open(const folly::fbstring &fileId,
    const int /*flags*/, const Params & /*openParams*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto handle =
        std::make_shared<WebDAVFileHandle>(fileId, shared_from_this());

    return connect().then(
        [handle = std::move(handle)] { return folly::makeFuture(handle); });
}

folly::Future<folly::Unit> WebDAVHelper::access(
    const folly::fbstring &fileId, const int /*mask*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.access");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVPROPFIND>(*self, evb);
        folly::fbvector<folly::fbstring> propFilter;

        return (*request)(fileId, 0, propFilter).then(evb, [
            &nsMap = self->m_nsMap, fileId, request
        ](PAPtr<pxml::Document> && /*multistatus*/) {
            return folly::makeFuture();
        });
    });
}

folly::Future<struct stat> WebDAVHelper::getattr(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.getattr");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<struct stat>(ECANCELED);

        auto request = std::make_shared<WebDAVPROPFIND>(*self, evb);
        folly::fbvector<folly::fbstring> propFilter;

        return (*request)(fileId, 0, propFilter).then([
            &nsMap = self->m_nsMap, fileId, request
        ](PAPtr<pxml::Document> && multistatus) {
            struct stat attrs {
            };

            auto resourceType = multistatus->getNodeByPathNS(
                "d:multistatus/d:response/d:propstat/d:prop/"
                "d:resourcetype",
                nsMap);

            if (!resourceType->hasChildNodes()) {
                // Regular file
                attrs.st_mode = S_IFREG;
            }
            else {
                // Collection
                attrs.st_mode = S_IFDIR;
            }

            auto getLastModified = multistatus->getNodeByPathNS(
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
                    attrs.st_ctim.tv_sec = dateTime.timestamp().epochTime();
                attrs.st_atim.tv_nsec = attrs.st_mtim.tv_nsec =
                    attrs.st_ctim.tv_nsec = 0;
            }

            auto getContentLength = multistatus->getNodeByPathNS(
                "d:multistatus/d:response/d:propstat/d:prop/"
                "d:getcontentlength",
                nsMap);

            if (getContentLength != nullptr) {
                attrs.st_size = std::stoi(getContentLength->innerText());
            }

            return attrs;
        });
    });
}

folly::Future<folly::Unit> WebDAVHelper::unlink(
    const folly::fbstring &fileId, const size_t /*currentSize*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.unlink");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVDELETE>(*self, evb);

        return (*request)(fileId).then(
            [request]() { return folly::makeFuture(); });
    });
}

folly::Future<folly::Unit> WebDAVHelper::rmdir(const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.rmdir");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVDELETE>(*self, evb);

        return (*request)(fileId).then(
            [request]() { return folly::makeFuture(); });
    });
}

folly::Future<folly::Unit> WebDAVHelper::truncate(
    const folly::fbstring &fileId, off_t size, const size_t currentSize)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(size);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.truncate");

    if (size > 0) {
        if (static_cast<size_t>(size) == currentSize)
            return folly::makeFuture();

        if (static_cast<size_t>(size) < currentSize)
            return makeFuturePosixException(ERANGE);
    }

    return connect().then([
        fileId, timer = std::move(timer), size, currentSize,
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVPUT>(*self, evb);

        if (size == 0) {
            return (*request)(fileId, size, folly::IOBuf::create(0))
                .then([request]() { return folly::makeFuture(); });
        }

        auto fillBuf = folly::IOBuf::create(size - currentSize);
        for (auto i = 0u; i < static_cast<size_t>(size) - currentSize; i++)
            fillBuf->writableData()[i] = 0;
        return (*request)(fileId, size, std::move(fillBuf)).then([request]() {
            return folly::makeFuture();
        });
    });
}

folly::Future<folly::Unit> WebDAVHelper::mknod(const folly::fbstring &fileId,
    const mode_t /*mode*/, const FlagsSet & /*flags*/, const dev_t /*rdev*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.mknod");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVPUT>(*self, evb);

        return (*request)(fileId, 0, std::make_unique<folly::IOBuf>())
            .then([request] { return folly::makeFuture(); });
    });
}

folly::Future<folly::Unit> WebDAVHelper::mkdir(
    const folly::fbstring &fileId, const mode_t /*mode*/)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.mkdir");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVMKCOL>(*self, evb);

        return (*request)(fileId).then(
            [request]() { return folly::makeFuture(); });
    });
}

folly::Future<folly::Unit> WebDAVHelper::rename(
    const folly::fbstring &from, const folly::fbstring &to)
{
    LOG_FCALL() << LOG_FARG(from) << LOG_FARG(to);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.rename");

    return connect().then([
        from, to, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVMOVE>(*self, evb);

        return (*request)(from, to).then(
            [request]() { return folly::makeFuture(); });
    });
}

folly::Future<folly::fbvector<folly::fbstring>> WebDAVHelper::readdir(
    const folly::fbstring &fileId, off_t offset, size_t count)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(offset) << LOG_FARG(count);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.readdir");

    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                ECANCELED);

        auto request = std::make_shared<WebDAVPROPFIND>(*self, evb);
        folly::fbvector<folly::fbstring> propFilter;

        return (*request)(fileId, 1, propFilter).then([
            &nsMap = self->m_nsMap, fileId, request
        ](PAPtr<pxml::Document> && multistatus) {
            try {
                PAPtr<pxml::NodeList> responses =
                    multistatus->getElementsByTagNameNS(kNSDAV, "response");

                folly::fbvector<folly::fbstring> result;
                result.reserve(responses->length());

                std::vector<folly::StringPiece> fileIdElements;
                folly::split("/", fileId, fileIdElements);

                auto entryCount = responses->length();

                auto directoryPath = ensureCollectionPath(fileId);

                for (auto i = 0u; i < entryCount; i++) {
                    auto response = responses->item(i);
                    if (response == nullptr || !response->hasChildNodes())
                        continue;

                    nsMap.declarePrefix("d", kNSDAV);
                    auto href = response->getNodeByPathNS("d:href", nsMap);

                    if (href == nullptr)
                        continue;

                    std::vector<folly::StringPiece> pathElements;
                    folly::split("/", href->innerText(), pathElements);

                    if (!pathElements.empty() &&
                        (directoryPath != href->innerText())) {
                        result.emplace_back(pathElements.back().toString());
                    }
                }

                return result;
            }
            catch (std::exception &e) {
                LOG(ERROR) << "Invalid response from server when trying to "
                              "read directory "
                           << fileId << ": " << e.what();
                throw makePosixException(EIO);
            }
        });
    });
}

folly::Future<folly::fbstring> WebDAVHelper::getxattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.getxattr");

    return connect().then([
        fileId, name, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::fbstring>(ECANCELED);

        auto request = std::make_shared<WebDAVPROPFIND>(*self, evb);
        folly::fbvector<folly::fbstring> propFilter;

        return (*request)(fileId, 0, propFilter).then([
            &nsMap = self->m_nsMap, fileId, name, request
        ](PAPtr<pxml::Document> && multistatus) {
            std::string nameEncoded;
            Poco::URI::encode(name.toStdString(), "", nameEncoded);
            auto attributeNode = multistatus->getNodeByPathNS(
                folly::sformat("d:multistatus/d:response/d:propstat/d:prop/"
                               "o:{}",
                    nameEncoded),
                nsMap);

            folly::fbstring result;

            if ((attributeNode != nullptr) &&
                (attributeNode->childNodes()->length() == 1)) {
                auto attributeValueNode = attributeNode->firstChild();
                if ((attributeValueNode != nullptr) &&
                    (attributeValueNode->nodeType() == pxml::Node::TEXT_NODE)) {
                    result = attributeNode->innerText();
                }
                else {
                    LOG(WARNING) << "Unprocessable " << name
                                 << " property value returned for " << fileId;
                }
            }
            else {
                throw makePosixException(ENODATA);
            }

            return result;
        });
    });
}

folly::Future<folly::Unit> WebDAVHelper::setxattr(const folly::fbstring &fileId,
    const folly::fbstring &name, const folly::fbstring &value, bool /*create*/,
    bool /*replace*/)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name) << LOG_FARG(value);

    auto timer = ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.setxattr");

    return connect().then([
        fileId, name, value, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVPROPPATCH>(*self, evb);

        return (*request)(fileId, name, value, false)
            .then([fileId, name, request]() {});
    });
}

folly::Future<folly::Unit> WebDAVHelper::removexattr(
    const folly::fbstring &fileId, const folly::fbstring &name)
{
    LOG_FCALL() << LOG_FARG(fileId) << LOG_FARG(name);

    auto timer =
        ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.removexattr");

    return connect().then([
        fileId, name, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::Unit>(ECANCELED);

        auto request = std::make_shared<WebDAVPROPPATCH>(*self, evb);

        return (*request)(fileId, name, "", true)
            .then([fileId, name, request]() {});
    });
}

folly::Future<folly::fbvector<folly::fbstring>> WebDAVHelper::listxattr(
    const folly::fbstring &fileId)
{
    LOG_FCALL() << LOG_FARG(fileId);

    auto timer =
        ONE_METRIC_TIMERCTX_CREATE("comp.helpers.mod.webdav.listxattr");

    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
    return connect().then([
        fileId, timer = std::move(timer),
        s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ](folly::EventBase * evb) {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::fbvector<folly::fbstring>>(
                ECANCELED);

        auto request = std::make_shared<WebDAVPROPFIND>(*self, evb);
        folly::fbvector<folly::fbstring> propFilter;

        return (*request)(fileId, 0, propFilter).then([
            &nsMap = self->m_nsMap, fileId, request
        ](PAPtr<pxml::Document> && multistatus) {
            folly::fbvector<folly::fbstring> result;

            auto prop = multistatus->getNodeByPathNS(
                "d:multistatus/d:response/d:propstat/d:prop", nsMap);

            if (prop != nullptr) {
                for (auto i = 0u; i < prop->childNodes()->length(); i++) {
                    auto property = prop->childNodes()->item(i);

                    // Filter out Onedata extended attributes
                    if (property->namespaceURI() == kNSOnedata) {
                        result.emplace_back(property->localName());
                    }
                }
            }

            return result;
        });
    });
}

folly::Future<folly::EventBase *> WebDAVHelper::connect()
{
    LOG_FCALL();

    // Select an event base to schedule the operations directly
    folly::EventBase *evb = m_executor->getEventBase();

    assert(evb != nullptr);

    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
    return folly::via(evb, [
        this, evb, s = std::weak_ptr<WebDAVHelper>{shared_from_this()}
    ]() mutable {
        auto self = s.lock();
        if (!self)
            return makeFuturePosixException<folly::EventBase *>(ECANCELED);

        std::lock_guard<std::mutex> guard{m_context->connectionMutex};

        if (m_context->connectionPromise.isFulfilled()) {
            // NOLINTNEXTLINE
            return folly::via(evb, [evb]() { return evb; });
        }

        if (connector() == nullptr) {
            m_context->timer = folly::HHWheelTimer::newTimer(evb,
                std::chrono::milliseconds(
                    folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
                folly::AsyncTimeout::InternalEnum::NORMAL, m_timeout);

            m_context->connector =
                std::make_unique<proxygen::HTTPConnector>(this, timer());
        }

        // Check if we are already connecting on this thread
        if (!self->connector()->isBusy()) {
            folly::SocketAddress address{
                m_endpoint.getHost(), m_endpoint.getPort(), true};

            LOG_DBG(2) << "Connecting to " << m_endpoint.getHost() << ":"
                       << m_endpoint.getPort();

            static const folly::AsyncSocket::OptionMap socketOptions{
                {{SOL_SOCKET, SO_REUSEADDR}, 1}};

            if (m_endpoint.getScheme() == "https") {
                auto context = std::make_shared<folly::SSLContext>();

                context->authenticate(m_verifyServerCertificate, false);

                folly::ssl::setSignatureAlgorithms<
                    folly::ssl::SSLCommonOptions>(*context);

                context->setVerificationOption(m_verifyServerCertificate
                        ? folly::SSLContext::SSLVerifyPeerEnum::VERIFY
                        : folly::SSLContext::SSLVerifyPeerEnum::NO_VERIFY);

                auto sslCtx = context->getSSLCtx();
                SSL_CTX_set_default_verify_paths(sslCtx);

                // NOLINTNEXTLINE
                SSL_CTX_set_session_cache_mode(sslCtx,
                    SSL_CTX_get_session_cache_mode(sslCtx) |
                        SSL_SESS_CACHE_CLIENT);

                connector()->connectSSL(evb, address, context, nullptr,
                    m_timeout, socketOptions, folly::AsyncSocket::anyAddress(),
                    m_endpoint.getHost());
            }
            else {
                connector()->connect(evb, address, m_timeout, socketOptions);
            }
        }

        return m_context->connectionPromise.getFuture().then(
            evb, [evb]() mutable {
                // NOLINTNEXTLINE
                return folly::makeFuture<folly::EventBase *>(std::move(evb));
            });
    });
}

void WebDAVHelper::connectSuccess(proxygen::HTTPUpstreamSession *session)
{

    assert(session != nullptr);

    // TODO: mutex
    m_context->session = session;
    session->setMaxConcurrentIncomingStreams(1);
    session->setMaxConcurrentOutgoingStreams(1);
    m_context->tokenPool.init(1);
    m_context->connectionPromise.setValue();
}

void WebDAVHelper::connectError(const folly::AsyncSocketException &ex)
{
    LOG(ERROR) << "Error when connecting to " + m_endpoint.toString() + ": " +
            ex.what();

    m_context->session = nullptr;

    // TODO: retry
}

/**
 * WebDAVRequest base
 */
WebDAVRequest::WebDAVRequest(const WebDAVHelper &helper, folly::EventBase *evb)
    : m_session{helper.session()}
    , m_tokenPool{helper.tokenPool()}
    , m_token{}
    , m_txn{nullptr}
    , m_evb{evb}
    , m_resultCode{}
    , m_resultBody{std::make_unique<folly::IOBufQueue>(
          folly::IOBufQueue::cacheChainLength())}
{
    assert(m_session != nullptr);

    m_request.setHTTPVersion(kWebDAVHTTPVersionMajor, kWebDAVHTTPVersionMinor);
    if (m_request.getHeaders().getNumberOfValues("Host") == 0u) {
        m_request.getHeaders().add(
            "Host", m_session->getLocalAddress().getHostStr());
    }
    if (m_request.getHeaders().getNumberOfValues("User-Agent") == 0u) {
        m_request.getHeaders().add("User-Agent", "Onedata");
    }
    if (m_request.getHeaders().getNumberOfValues("Accept") == 0u) {
        m_request.getHeaders().add("Accept", "*/*");
    }
    if (m_request.getHeaders().getNumberOfValues("Authorization") == 0u) {
        if (helper.credentialsType() == WebDAVCredentialsType::BASIC) {
            std::stringstream b64Stream;
            Poco::Base64Encoder b64Encoder(b64Stream);
            b64Encoder << helper.credentials();
            b64Encoder.close();
            m_request.getHeaders().add(
                "Authorization", folly::sformat("Basic {}", b64Stream.str()));
        }
        else if (helper.credentialsType() == WebDAVCredentialsType::TOKEN) {
            std::vector<folly::StringPiece> authHeader;
            folly::split(":", helper.authorizationHeader(), authHeader);
            if (authHeader.size() == 1)
                m_request.getHeaders().add(
                    folly::sformat("{}", folly::trimWhitespace(authHeader[0])),
                    helper.credentials().toStdString());
            else if (authHeader.size() == 2) {
                m_request.getHeaders().add(folly::sformat("{}", authHeader[0]),
                    folly::sformat(authHeader[1], helper.credentials()));
            }
            else {
                LOG(WARNING) << "Unexpected token authorization header value: "
                             << helper.authorizationHeader();
            }
        }
    }
}

folly::Future<proxygen::HTTPTransaction *> WebDAVRequest::startTransaction()
{
    assert(m_evb != nullptr);

    return m_tokenPool->get().via(m_evb).then(
        [this](HTTPTransactionToken &&token) {
            m_token = token;
            auto maxOutcomingStreams =
                m_session->getMaxConcurrentOutgoingStreams();
            auto maxHistOutcomingStreams =
                m_session->getHistoricalMaxOutgoingStreams();
            auto outcomingStreams = m_session->getNumOutgoingStreams();
            auto processedTransactions = m_session->getNumTxnServed();

            LOG_DBG(4) << "Session (" << m_session
                       << ") stats: " << maxHistOutcomingStreams << ", "
                       << maxOutcomingStreams << ", " << outcomingStreams
                       << ", " << processedTransactions;

            auto txn = m_session->newTransaction(this);
            assert(txn != nullptr);
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
    m_tokenPool->putBack(std::move(m_token)); // NOLINT
    m_destructionGuard.reset();
}

void WebDAVRequest::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept
{
    m_resultCode = msg->getStatusCode();
}

void WebDAVRequest::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept {}

void WebDAVRequest::onTrailers(
    std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept
{
}

void WebDAVRequest::onEOM() noexcept {}

void WebDAVRequest::onUpgrade(proxygen::UpgradeProtocol protocol) noexcept {}

void WebDAVRequest::onEgressPaused() noexcept {}

void WebDAVRequest::onEgressResumed() noexcept {}

/**
 * GET
 */
folly::Future<folly::IOBufQueue> WebDAVGET::operator()(
    const folly::fbstring &resource, const off_t offset, const size_t size)
{
    if (size == 0)
        return folly::makeFuture<folly::IOBufQueue>(
            folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()});

    m_request.setMethod("GET");
    m_request.rawSetURL(ensureHttpPath(resource));
    if (offset == 0 && size == 1) {
        m_firstByteRequest = true;
        m_request.getHeaders().add("Range", folly::sformat("bytes=0-1"));
    }
    else {
        m_request.getHeaders().add(
            "Range", folly::sformat("bytes={}-{}", offset, offset + size - 1));
    }

    m_destructionGuard = shared_from_this();

    return startTransaction().then([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVGET::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept
{
    m_resultBody->append(std::move(chain));
}

void WebDAVGET::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

void WebDAVGET::onEOM() noexcept
{
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

/**
 * PUT - only for servers supporting Content-Range header
 */
folly::Future<folly::Unit> WebDAVPUT::operator()(
    const folly::fbstring &resource, const off_t offset,
    std::unique_ptr<folly::IOBuf> buf)
{
    m_request.setMethod("PUT");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));
    if (buf->length() > 0) {
        m_request.getHeaders().add("Content-Range",
            folly::sformat(
                "bytes {}-{}/*", offset, offset + buf->length() - 1));
    }

    m_destructionGuard = shared_from_this();

    return startTransaction().then([ this, buf = std::move(buf) ](
        proxygen::HTTPTransaction * txn) mutable {
        txn->sendHeaders(m_request);
        txn->sendBody(std::move(buf));
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVPUT::onEOM() noexcept
{
    auto result = httpStatusToPosixError(m_resultCode);
    if (result == 0) {
        m_resultPromise.setValue();
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVPUT::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
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
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));
    m_request.getHeaders().add("X-Update-Range",
        folly::sformat("bytes={}-{}", offset, offset + buf->length() - 1));
    m_request.getHeaders().add(
        "Content-type", "application/x-sabredav-partialupdate");
    m_request.getHeaders().add(
        "Content-length", folly::sformat("{}", buf->length()));

    m_destructionGuard = shared_from_this();

    return startTransaction().then([ this, buf = std::move(buf) ](
        proxygen::HTTPTransaction * txn) mutable {
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
    auto result = httpStatusToPosixError(m_resultCode);
    if (result == 0) {
        m_resultPromise.setValue();
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVPATCH::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

/**
 * MKCOL
 */
folly::Future<folly::Unit> WebDAVMKCOL::operator()(
    const folly::fbstring &resource)
{
    m_request.setMethod("MKCOL");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));

    m_destructionGuard = shared_from_this();

    return startTransaction().then([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVMKCOL::onEOM() noexcept
{
    auto result = httpStatusToPosixError(m_resultCode);
    if (result == 0) {
        m_resultPromise.setValue();
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVMKCOL::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

/**
 * PROPFIND
 */
folly::Future<PAPtr<pxml::Document>> WebDAVPROPFIND::operator()(
    const folly::fbstring &resource, const int depth,
    const folly::fbvector<folly::fbstring> & /*propFilter*/)
{
    m_request.setMethod("PROPFIND");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));
    m_request.getHeaders().add(
        "Depth", (depth >= 0) ? std::to_string(depth) : "infinity");

    m_destructionGuard = shared_from_this();

    return startTransaction().then(
        m_evb, [this](proxygen::HTTPTransaction *txn) {
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
    auto result = httpStatusToPosixError(m_resultCode);

    if (result == 0) {
        m_resultPromise.setWith([bufq = std::move(m_resultBody)]() {
            bufq->gather(bufq->chainLength());
            auto iobuf = bufq->empty() ? folly::IOBuf::create(0) : bufq->move();
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

void WebDAVPROPFIND::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

/**
 * PROPPATCH
 */
folly::Future<folly::Unit> WebDAVPROPPATCH::operator()(
    const folly::fbstring &resource, const folly::fbstring &property,
    const folly::fbstring &value, const bool remove)
{
    m_request.setMethod("PROPPATCH");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));
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

    return startTransaction().then([ this, body = std::move(propUpdateString) ](
        proxygen::HTTPTransaction * txn) {
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
    auto result = httpStatusToPosixError(m_resultCode);

    if (result == 0) {
        m_resultPromise.setWith([bufq = std::move(m_resultBody)]() {
            bufq->gather(bufq->chainLength());
            auto iobuf = bufq->empty() ? folly::IOBuf::create(0) : bufq->move();
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

                auto status = xml->getNodeByPathNS(
                    "d:multistatus/d:response/d:propstat/d:status", nsMap);

                if (status == nullptr) {
                    throw makePosixException(EINVAL);
                }

                if ((std::string("HTTP/1.1 200 OK") == status->innerText()) ||
                    (std::string("HTTP/1.1 204 No Content") ==
                        status->innerText())) {
                    return folly::Unit{};
                }

                throw makePosixException(EINVAL);
            }
            catch (std::exception &e) {
                LOG(ERROR) << "Cannot parse PROPPATCH response: " << e.what();
                throw makePosixException(EIO);
            }
        });
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVPROPPATCH::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

/**
 * DELETE
 */
folly::Future<folly::Unit> WebDAVDELETE::operator()(
    const folly::fbstring &resource)
{
    m_request.setMethod("DELETE");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));

    m_destructionGuard = shared_from_this();

    return startTransaction().then([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVDELETE::onEOM() noexcept
{
    auto result = httpStatusToPosixError(m_resultCode);
    if (result == 0) {
        m_resultPromise.setValue();
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVDELETE::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

/**
 * MOVE
 */
folly::Future<folly::Unit> WebDAVMOVE::operator()(
    const folly::fbstring &resource, const folly::fbstring &destination)
{
    m_request.setMethod("MOVE");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));
    m_request.getHeaders().add("Destination", destination.toStdString());

    m_destructionGuard = shared_from_this();

    return startTransaction().then([this](proxygen::HTTPTransaction *txn) {
        txn->sendHeaders(m_request);
        txn->sendEOM();
        return m_resultPromise.getFuture();
    });
}

void WebDAVMOVE::onEOM() noexcept
{
    auto result = httpStatusToPosixError(m_resultCode);
    if (result == 0) {
        m_resultPromise.setValue();
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVMOVE::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

/**
 * COPY
 */
folly::Future<folly::Unit> WebDAVCOPY::operator()(
    const folly::fbstring &resource, const folly::fbstring &destination)
{
    m_request.setMethod("COPY");
    m_request.rawSetURL(ensureHttpPath(resource.toStdString()));
    m_request.getHeaders().add("Destination", destination.toStdString());

    m_destructionGuard = shared_from_this();

    auto txn = m_session->newTransaction(this);
    txn->sendHeaders(m_request);
    txn->sendEOM();

    return m_resultPromise.getFuture();
}

void WebDAVCOPY::onEOM() noexcept
{
    auto result = httpStatusToPosixError(m_resultCode);
    if (result == 0) {
        m_resultPromise.setValue();
    }
    else {
        m_resultPromise.setException(makePosixException(result));
    }
}

void WebDAVCOPY::onError(const proxygen::HTTPException &error) noexcept
{
    m_resultPromise.setException(error);
}

} // namespace helpers
} // namespace one
