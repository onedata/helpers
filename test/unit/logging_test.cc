/**
 * @file logging_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "logging.h"
#include "testUtils.h"

#include "boost/algorithm/string.hpp"
#include <gtest/gtest.h>

#include <sstream>
#include <string>

using namespace ::testing;
using namespace one;
using namespace one::logging;

struct LoggingTest : public ::testing::Test {
    LoggingTest() {}

    ~LoggingTest() {}

    void SetUp() override {}

    void TearDown() override {}
};

std::string function2()
{
    std::stringstream logSink;
    LOG_STACKTRACE(logSink, "Test stacktrace:");
    return logSink.str();
}

std::string function1() { return function2(); }

TEST_F(LoggingTest, loggingStackTraceShouldWork)
{
    auto log = function1();

    ASSERT_TRUE(boost::contains(log, "Test stacktrace:"));
    ASSERT_TRUE(boost::contains(log, "function1"));
    ASSERT_TRUE(boost::contains(log, "function2"));
}
