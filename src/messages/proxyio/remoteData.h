/**
 * @file remoteData.h
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_MESSAGES_PROXYIO_REMOTE_DATA_H
#define HELPERS_MESSAGES_PROXYIO_REMOTE_DATA_H

#include "proxyIOResponse.h"

#include <memory>
#include <string>

namespace one {
namespace messages {
namespace proxyio {

/**
 * The @c RemoteData class represents a result of a @c RemoteRead request.
 */
class RemoteData : public ProxyIOResponse {
public:
    /**
     * Constructor.
     * @param serverMessage Protocol Buffers message representing
     * @c RemoteData counterpart.
     */
    explicit RemoteData(std::unique_ptr<ProtocolServerMessage> serverMessage);

    /**
     * @return The data.
     */
    const std::string &data() const { return m_data; }

    /**
     * @return The data, mutable.
     */
    std::string &mutableData() { return m_data; }

    std::string toString() const override;

private:
    std::string m_data;
};

} // namespace proxyio
} // namespace messages
} // namespace one

#endif // HELPERS_MESSAGES_PROXYIO_REMOTE_DATA_H
