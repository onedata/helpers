/**
 * @file httpHelper_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2026 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "httpHelper.h"
#include "testUtils.h"

#include <folly/FBString.h>
#include <folly/Singleton.h>
#include <gtest/gtest.h>

using namespace ::testing;
using namespace one;
using namespace one::helpers;
using namespace one::testing;

struct HTTPHelperTest : public ::testing::Test {
    HTTPHelperTest()
    {
        folly::SingletonVault::singleton()->registrationComplete();
    }

    ~HTTPHelperTest() { }

    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(HTTPHelperTest, parseContentRangeShouldParseValidHeader)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("bytes 0-499/1234", r));
    EXPECT_EQ(r.first, 0);
    EXPECT_EQ(r.last, 499);
    EXPECT_EQ(r.total, 1234U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseHeaderWithWildcardTotal)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("bytes 0-499/*", r));
    EXPECT_EQ(r.first, 0);
    EXPECT_EQ(r.last, 499);
    EXPECT_EQ(r.total, 0U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseHeaderWithLeadingWhitespace)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("  bytes 100-199/500", r));
    EXPECT_EQ(r.first, 100);
    EXPECT_EQ(r.last, 199);
    EXPECT_EQ(r.total, 500U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseHeaderWithTrailingWhitespace)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("bytes 100-199/500  ", r));
    EXPECT_EQ(r.first, 100);
    EXPECT_EQ(r.last, 199);
    EXPECT_EQ(r.total, 500U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldBeCaseInsensitive)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("BYTES 0-999/5000", r));
    EXPECT_EQ(r.first, 0);
    EXPECT_EQ(r.last, 999);
    EXPECT_EQ(r.total, 5000U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseSingleByteRange)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("bytes 42-42/100", r));
    EXPECT_EQ(r.first, 42);
    EXPECT_EQ(r.last, 42);
    EXPECT_EQ(r.total, 100U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseRangeWithoutUnits)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("1024-2048/36772086", r));
    EXPECT_EQ(r.first, 1024);
    EXPECT_EQ(r.last, 2048);
    EXPECT_EQ(r.total, 36772086U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldRejectInvalidFormat)
{
    detail::ContentRange r;
    EXPECT_FALSE(detail::parseContentRange("", r));
    EXPECT_FALSE(detail::parseContentRange("invalid", r));
    EXPECT_FALSE(detail::parseContentRange("bytes 0-499", r));
    EXPECT_FALSE(detail::parseContentRange("bytes/0-499/1234", r));
}

TEST_F(HTTPHelperTest, parseContentRangeShouldRejectReversedRange)
{
    detail::ContentRange r;
    EXPECT_FALSE(detail::parseContentRange("bytes 500-0/1234", r));
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseLargeOffsets)
{
    detail::ContentRange r;
    EXPECT_TRUE(
        detail::parseContentRange("bytes 1000000000-1999999999/5000000000", r));
    EXPECT_EQ(r.first, 1000000000);
    EXPECT_EQ(r.last, 1999999999);
    EXPECT_EQ(r.total, 5000000000U);
}

TEST_F(HTTPHelperTest, parseContentRangeShouldParseZeroLengthFile)
{
    detail::ContentRange r;
    EXPECT_TRUE(detail::parseContentRange("bytes 0-0/1", r));
    EXPECT_EQ(r.first, 0);
    EXPECT_EQ(r.last, 0);
    EXPECT_EQ(r.total, 1U);
}
