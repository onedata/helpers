/**
 * @file xrootdHelper_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2024 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "testUtils.h"
#include "xrootdHelper.h"

#include <boost/make_shared.hpp>
#include <folly/Singleton.h>
#include <folly/executors/GlobalExecutor.h>
#include <gtest/gtest.h>

#include <tuple>

#include <boost/algorithm/string.hpp>
#include <folly/String.h>

using namespace ::testing;
using namespace one;
using namespace one::helpers;
using namespace one::testing;

struct XRootDHelperTest : public ::testing::Test {
    XRootDHelperTest()
    {
        folly::SingletonVault::singleton()->registrationComplete();
    }

    ~XRootDHelperTest() { }

    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(XRootDHelperTest, xrootdappendPathToURLShouldWork)
{
    {
        XrdCl::URL base = "root://example.com:1111/";

        XrdCl::URL target = appendPathToURL(base, "/data");

        EXPECT_EQ(target.GetURL(), "root://example.com:1111//data");
    }

    {
        XrdCl::URL base = "root://example.com:1111/?xrd.logintoken=ABCDEFG";

        XrdCl::URL target = appendPathToURL(base, "/data");

        EXPECT_EQ(target.GetURL(),
            "root://example.com:1111//data?xrd.logintoken=ABCDEFG");
    }
}
