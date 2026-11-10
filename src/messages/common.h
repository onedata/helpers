/**
 * @file common.h
 * @author Bartek Kryza
 * @copyright (C) 2023 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_MESSAGES_COMMON_H
#define HELPERS_MESSAGES_COMMON_H

namespace one {
namespace messages {
namespace handshake {
enum class SessionMode { normal = 1, open_handle };
enum class ClientType { oneclient, onedatafs, ones3 };
} // namespace handshake
} // namespace messages
} // namespace one

#endif // ONECLIENT_MESSAGES_COMMON_H
