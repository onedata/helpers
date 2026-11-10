/**
 * @file proxyIORequest.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_MESSAGES_PROXYIO_PROXYIO_REQUEST_H
#define HELPERS_MESSAGES_PROXYIO_PROXYIO_REQUEST_H

#include "messages/clientMessage.h"

#include <folly/FBString.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace one {
namespace messages {
namespace proxyio {

/**
 * The @c ProxyIORequest class represents a request via ProxyIO.
 */
class ProxyIORequest : public ClientMessage {
public:
    /**
     * Constructor.
     * @param parameters Map of additional parameters.
     * @param storageId The ID of storage the file is stored on.
     * @param fileId The ID of file that will be manipulated.
     */
    ProxyIORequest(
        std::unordered_map<folly::fbstring, folly::fbstring> parameters,
        folly::fbstring storageId, folly::fbstring fileId);

    ~ProxyIORequest() override = default;

    const std::unordered_map<folly::fbstring, folly::fbstring> &
    parameters() const
    {
        return m_parameters;
    }

    folly::fbstring storageId() const { return m_storageId; }

    folly::fbstring fileId() const { return m_fileId; }

protected:
    std::unique_ptr<ProtocolClientMessage> serializeAndDestroy() override;

private:
    std::unordered_map<folly::fbstring, folly::fbstring> m_parameters;
    folly::fbstring m_storageId;
    folly::fbstring m_fileId;
};

} // namespace proxyio
} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_PROXYIO_PROXYIO_REQUEST_H
