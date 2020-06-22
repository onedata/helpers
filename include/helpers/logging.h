/**
 * @file logging.h
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include <boost/algorithm/string.hpp>
#include <boost/current_function.hpp>
#include <boost/range/adaptor/map.hpp>
#include <folly/Conv.h>
#include <glog/logging.h>

#include <bitset>
#include <cxxabi.h>
#include <execinfo.h>
#include <limits>
#include <map>
#include <unordered_map>

#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

#pragma once

// clang-format off
/**
 * This file provides a set of logging macros to be used in helpers and
 * oneclient components.
 *
 * The macros are wrappers over Google GLOG library logging facilities. The
 * following rules should be followed when adding logs in the code:
 *  - Runtime info, warning and error logs which will be included in release
 *    builds should be logged using LOG(INFO), LOG(WARNING) and LOG(ERROR)
 *    macros. They should be kept to minimum and provide information on
 *    errors and their possible mitigation.
 *  - Verbose logs should be logged using GLOG verbose logging facility,
 *    i.e. VLOG(n) macros. Below are several convenience macros which should
 *    be used in the code instead of GLOG native macros:
 *     * LOG_DBG(n) - log with specific verbosity level
 *     * LOG_FCALL() - log function signature, should be used at the beginning
 *                     of functions and methods, which are relevant for the
 *                     program flow (they will be logged at level 3)
 *     * LOG_FARG(arg) - appends to LOG_FCALL() name and value of a specific
 *                       function argument, example use:
 *                       LOG_FCALL() << LOG_FARG(arg1) << LOG_FARG(arg2)
 *     * LOG_FARGx(arg) - allows to easily log more complex values, such as
 *                        numbers in different numeric bases (LOG_FARGB,
 *                        LOG_FARGO, LOG_FARGH), vectors and lists (LOG_FARGV)
 *                        and maps (LOG_FARGM)
 *  - Stack traces can be logged at any place in the code using:
 *     * LOG_STACKTRACE(ostream, msg) - where ostream is any valid ostream,
 *                                      including e.g. LOG(ERROR) or LOG_DBG(1)
 *                                      and msg is a header added to the stack
 *                                      trace
 *    Debug logs can be enabled by setting global GLOG variables:
 *     * FLAGS_v = n; - where n determines the verbosity level
 */
// clang-format on
/**
 * Logs a value in binary format
 */
#define LOG_BIN(X)                                                             \
    std::bitset<std::numeric_limits<decltype(X)>::digits>(X) << "b"

/**
 * Logs a value in octal format
 */
#define LOG_OCT(X) "0" << std::oct << X << std::dec

/**
 * Logs a value in hexadecimal format
 */
#define LOG_HEX(X) "0x" << std::hex << X << std::dec

/**
 * Appends to stream a serialized vector of strings
 */
#define LOG_VEC(X) "[" << boost::algorithm::join(X, ",") << "]"

/**
 * Appends to stream a serialized map in the form:
 * {key1,key2,...,keyN} => {val1,val2,...,valN}
 */
#define LOG_MAP(X) one::logging::mapToString(X)

/**
 * Encodes a string into a comma-separate int format compatible
 * with Erlang binary logs.
 */
#define LOG_ERL_BIN(X) one::logging::containerToErlangBinaryString(X)

/**
 * Macro for verbose logging in debug mode
 */
#define LOG_DBG(X) VLOG(X)

/**
 * Logs function call, should be added at the beginning of the function or
 * method body and log the values of main parameters.
 */
// clang-format off
#define LOG_TRACE(...) SPDLOG_TRACE(spdlog::get("default"), __VA_ARGS__)

#define LOG_FCALL()                                                             \
    VLOG(3) << "Called " << BOOST_CURRENT_FUNCTION << " with arguments: " // NOLINT
// clang-format on

/**
 * Logs function return including optionally the return value.
 */
#define LOG_FRET()                                                             \
    VLOG(3) << "Returning from " << BOOST_CURRENT_FUNCTION << " with value: "

/**
 * Logs function argument - must be used in 'stream' context and preceded by
 * LOG_FCALL() or VLOG(n).
 */
#define LOG_FARG(ARG) " " #ARG " = " << ARG

/**
 * Log macros for different numeric bases.
 */
#define LOG_FARGB(ARG) " " #ARG "=" << LOG_BIN(ARG)
#define LOG_FARGO(ARG) " " #ARG "=" << LOG_OCT(ARG)
#define LOG_FARGH(ARG) " " #ARG "=" << LOG_HEX(ARG)

/**
 * Logs function argument which is a vector - must be used in 'stream' context
 * and preceded by LOG_FCALL() or VLOG(n).
 */
#define LOG_FARGV(ARG) " " #ARG "=" << LOG_VEC(ARG)

/**
 * Logs function argument which is a map - must be used in 'stream' context
 * and preceded by LOG_FCALL() or VLOG(n).
 */
#define LOG_FARGM(ARG) " " #ARG "=" << LOG_MAP(ARG)

/**
 * Logs current stack trace, should be used in `catch` blocks.
 */
#define LOG_STACKTRACE(X, MSG)                                                 \
    LOG_DBG(X) << MSG << '\n' << ::one::logging::print_stacktrace();

namespace one {
namespace logging {

/**
 * Converts any map to a string for logging.
 */
template <typename TMap, typename TResult = std::string>
TResult mapToString(const TMap &map)
{
    TResult result = "{ ";
    for (const auto &kv : map) {
        result += folly::to<TResult>(kv.first) + " => " +
            folly::to<TResult>(kv.second) + ", ";
    }
    result = result.substr(0, result.size() - 2);
    result += " }";
    return result;
}

template <typename TSeq = std::string>
std::string containerToErlangBinaryString(const TSeq &bytes)
{
    std::vector<std::string> bytesValues;

    std::for_each(bytes.begin(), bytes.end(), [&](const char &byte) {
        return bytesValues.push_back(std::to_string((uint8_t)byte));
    });

    return std::string("<<") + boost::algorithm::join(bytesValues, ",") + ">>";
}

/**
 * Based on:
 *   stacktrace.h (c) 2008, Timo Bingmann from http://idlebox.net/
 *    published under the WTFPL v2.0
 *
 * Print a demangled stack backtrace of the caller function to ostream.
 */
static inline std::string print_stacktrace()
{
    std::stringstream out;

    constexpr auto max_frames = 63;
    void *addrlist[max_frames + 1];

    // retrieve current stack addresses
    int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void *));

    if (addrlen == 0) {
        out << "  <empty, possibly corrupt>\n";
        return out.str();
    }

    // resolve addresses into strings containing "filename(function+address)",
    // this array must be free()-ed
    char **symbollist = backtrace_symbols(addrlist, addrlen);

    // allocate string which will be filled with the demangled function name
    size_t funcnamesize = 256;
    char *funcname = (char *)malloc(funcnamesize);

    // iterate over the returned symbol lines. skip the first, it is the
    // address of this function.
    for (int i = 1; i < addrlen; i++) {
        char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

        // find parentheses and +address offset surrounding the mangled name:
        // ./module(function+0x15c) [0x8048a6d]
        for (char *p = symbollist[i]; *p; ++p) {
            if (*p == '(')
                begin_name = p;
            else if (*p == '+')
                begin_offset = p;
            else if (*p == ')' && begin_offset) {
                end_offset = p;
                break;
            }
        }

        if (begin_name && begin_offset && end_offset &&
            begin_name < begin_offset) {
            *begin_name++ = '\0';
            *begin_offset++ = '\0';
            *end_offset = '\0';

            // mangled name is now in [begin_name, begin_offset) and caller
            // offset in [begin_offset, end_offset). now apply
            // __cxa_demangle():
            int status;
            char *ret = abi::__cxa_demangle(
                begin_name, funcname, &funcnamesize, &status);
            if (status == 0) {
                funcname = ret; // use possibly realloc()-ed string
                out << "  " << symbollist[i] << " : " << funcname << "+"
                    << begin_offset << "\n";
            }
            else {
                // demangling failed. Output function name as a C function with
                // no arguments.
                out << "  " << symbollist[i] << " : " << begin_name << "()+"
                    << begin_offset << "\n";
            }
        }
        else {
            // couldn't parse the line? print the whole line.
            out << "  " << symbollist[i] << '\n';
        }
    }

    free(funcname);
    free(symbollist);

    return out.str();
}
}
}

// Definition of custom spdlog based loggers
namespace one {
namespace logging {

template <typename Clock = std::chrono::steady_clock> struct log_timer {
    log_timer()
        : startTimePoint(Clock::now())
    {
    }

    auto stop()
    {
        using namespace std::chrono;
        return duration_cast<microseconds>(Clock::now() - startTimePoint)
            .count();
    }

    std::chrono::time_point<Clock> startTimePoint;
};

namespace csv {

struct read_write_perf {
    static constexpr const char name[] = "read_write_perf";
    static constexpr const char header[] =
        "Time,File,Class,Operation,Offset,Size,Duration [us]";
    static constexpr const char fmt[] = "{},{},{},{},{},{}";
};

template <typename Tag> void register_logger(std::string path)
{
    using namespace std::chrono;
    auto timestamp =
        duration_cast<milliseconds>(system_clock::now().time_since_epoch())
            .count();
    spdlog::basic_logger_mt(Tag::name,
        path + "/" + Tag::name + "-" + std::to_string(timestamp) + ".csv");
    spdlog::get(Tag::name)->set_pattern("%v");
    spdlog::get(Tag::name)->info(Tag::header);
    spdlog::get(Tag::name)->set_pattern("%H:%M:%S.%f,%v");
    spdlog::get(Tag::name)->set_level(spdlog::level::off);
}

template <typename Tag, typename... Arg> void log(const Arg &... args)
{
    auto logger = spdlog::get(Tag::name);
    if (logger)
        logger->info(Tag::fmt, args...);
}
} // namespace csv
} // namespace logging
} // namespace one

