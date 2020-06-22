/**
 * @file logging.cc
 * @author Bartek Kryza
 * @copyright (C) 2020 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/logging.h"
#include "folly/FBString.h"

namespace one {
namespace logging {
namespace csv {
constexpr const char read_write_perf::name[];
constexpr const char read_write_perf::header[];
constexpr const char read_write_perf::fmt[];
}
}
}
std::ostream &operator<<(std::ostream &os, const folly::fbstring &c)
{
    return os << c;
}
