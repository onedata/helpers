/**
 * @file remoteRead.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_MESSAGES_PROXYIO_REMOTE_READ_H
#define HELPERS_MESSAGES_PROXYIO_REMOTE_READ_H

#include "proxyIORequest.h"

#include <folly/FBString.h>
#include <folly/FBVector.h>

#include <sys/types.h>

#include <cstdint>
#include <string>

namespace one {
namespace messages {
namespace proxyio {

class RemoteRead : public ProxyIORequest {
public:
    RemoteRead(std::unordered_map<folly::fbstring, folly::fbstring> parameters,
        folly::fbstring storageId, folly::fbstring fileId, off_t offset,
        std::size_t size);

    std::string toString() const override;

private:
    std::unique_ptr<ProtocolClientMessage> serializeAndDestroy() override;

    const off_t m_offset;
    const std::size_t m_size;
};

} // namespace proxyio
} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_PROXYIO_REMOTE_READ_H
