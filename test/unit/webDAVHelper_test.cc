/**
 * @file webDAVHelper_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "testUtils.h"
#include "webDAVHelper.h"

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

struct WebDAVHelperTest : public ::testing::Test {
    WebDAVHelperTest()
    {
        folly::SingletonVault::singleton()->registrationComplete();
    }

    ~WebDAVHelperTest() {}

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(WebDAVHelperTest, webDAVHelperShouldParseHTTPWebDAVURLs)
{
    WebDAVHelperFactory factory{folly::getIOExecutor()};

    Params p1;
    p1.emplace("type", "webdav");
    p1.emplace("name", "webdav");
    p1.emplace("endpoint", "172.17.0.2");
    p1.emplace("credentials", "admin:password");

    auto helper1 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p1));

    EXPECT_EQ(helper1->endpoint().getScheme(), "http");
    EXPECT_EQ(helper1->endpoint().getPort(), 80);

    Params p2;
    p2.emplace("type", "webdav");
    p2.emplace("name", "webdav");
    p2.emplace("endpoint", "http://172.17.0.2");
    p2.emplace("credentials", "admin:password");

    auto helper2 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p2));

    EXPECT_EQ(helper2->endpoint().getScheme(), "http");
    EXPECT_EQ(helper2->endpoint().getPort(), 80);

    Params p3;
    p3.emplace("type", "webdav");
    p3.emplace("name", "webdav");
    p3.emplace("endpoint", "172.17.0.2:80");
    p3.emplace("credentials", "admin:password");

    auto helper3 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p3));

    EXPECT_EQ(helper3->endpoint().getScheme(), "http");
    EXPECT_EQ(helper3->endpoint().getPort(), 80);

    Params p4;
    p4.emplace("type", "webdav");
    p4.emplace("name", "webdav");
    p4.emplace("endpoint", "172.17.0.2:8080/collection1");
    p4.emplace("credentials", "admin:password");
    p4.emplace("authorizationHeader", "");

    auto helper4 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p4));

    EXPECT_EQ(helper4->endpoint().getScheme(), "http");
    EXPECT_EQ(helper4->endpoint().getPort(), 8080);
}

TEST_F(WebDAVHelperTest, webDAVHelperShouldParseHTTPSWebDAVURLs)
{
    WebDAVHelperFactory factory{folly::getIOExecutor()};

    Params p1;
    p1.emplace("type", "webdav");
    p1.emplace("name", "webdav");
    p1.emplace("endpoint", "https://172.17.0.2");
    p1.emplace("credentials", "admin:password");

    auto helper1 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p1));

    EXPECT_EQ(helper1->endpoint().getScheme(), "https");
    EXPECT_EQ(helper1->endpoint().getPort(), 443);

    Params p2;
    p2.emplace("type", "webdav");
    p2.emplace("name", "webdav");
    p2.emplace("endpoint", "172.17.0.2:443");
    p2.emplace("credentials", "admin:password");

    auto helper2 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p2));

    EXPECT_EQ(helper2->endpoint().getScheme(), "https");
    EXPECT_EQ(helper2->endpoint().getPort(), 443);

    Params p3;
    p3.emplace("type", "webdav");
    p3.emplace("name", "webdav");
    p3.emplace("endpoint", "https://172.17.0.2:8080");
    p3.emplace("credentials", "admin:password");
    p3.emplace("connectionPoolSize", "123");
    p3.emplace("maximumUploadSize", "100000000000");

    auto helper3 = std::dynamic_pointer_cast<WebDAVHelper>(
        factory.createStorageHelper(p3));

    EXPECT_EQ(helper3->endpoint().getScheme(), "https");
    EXPECT_EQ(helper3->endpoint().getPort(), 8080);
    EXPECT_EQ(helper3->connectionPoolSize(), 123);
    EXPECT_EQ(helper3->maximumUploadSize(), 100'000'000'000);
}
