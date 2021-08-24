/**
 * @file subscriptionData.cc
 * @author Konrad Zemek
 * @copyright (C) 2015 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "subscriptionData.h"

namespace one {
namespace communication {

SubscriptionData::SubscriptionData(
    std::function<bool(const ServerMessage &, const bool)> p,
    std::function<void(const ServerMessage &)> c)
    : m_predicate(std::move(p))
    , m_callback(std::move(c))
{
}

} // namespace communication
} // namespace one
