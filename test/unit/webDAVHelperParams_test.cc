/**
 * @file webDAVHelperParams_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "testUtils.h"
#include "webDAVHelperParams.h"

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

struct WebDAVHelperParamsTest : public ::testing::Test {
    WebDAVHelperParamsTest()
    {
        folly::SingletonVault::singleton()->registrationComplete();
    }

    ~WebDAVHelperParamsTest() {}

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(WebDAVHelperParamsTest, webDAVHelperParamsShouldInitializeFromParams)
{
    Params p1;
    p1.emplace("type", "webdav");
    p1.emplace("name", "webdav");
    p1.emplace("endpoint", "https://172.17.0.2:8080/dav");
    p1.emplace("credentials", "admin:password");
    p1.emplace("credentialsType", "basic");
    p1.emplace("verifyServerCertificate", "true");
    p1.emplace("authorizationHeader", "X-Auth-Header: {}");
    p1.emplace("oauth2IdP", "github");
    p1.emplace("accessToken", "ABCD");
    p1.emplace("accessTokenTTL", "3600");
    p1.emplace("rangeWriteSupport", "sabredav");
    p1.emplace("connectionPoolSize", "16");
    p1.emplace("maximumUploadSize", "1024");
    p1.emplace("timeout", "30");
    p1.emplace("fileMode", "664");
    p1.emplace("dirMode", "2775");

    auto params = WebDAVHelperParams::create(p1);

    EXPECT_EQ(params->endpoint().getScheme(), "https");
    EXPECT_EQ(params->endpoint().getPort(), 8080);
    EXPECT_EQ(params->credentials(), "admin:password");
    EXPECT_EQ(params->credentialsType(), WebDAVCredentialsType::BASIC);
    EXPECT_TRUE(params->verifyServerCertificate());
    EXPECT_EQ(params->authorizationHeader(), "X-Auth-Header: {}");
    EXPECT_EQ(params->oauth2IdP(), "github");
    EXPECT_EQ(params->accessToken(), "ABCD");
    EXPECT_EQ(params->accessTokenTTL().count(), 3600);
    EXPECT_EQ(params->rangeWriteSupport(),
        WebDAVRangeWriteSupport::SABREDAV_PARTIALUPDATE);
    EXPECT_EQ(params->connectionPoolSize(), 16);
    EXPECT_EQ(params->maximumUploadSize(), 1024);
    EXPECT_EQ(params->timeout().count(), 30);
    EXPECT_EQ(params->fileMode(), 0664);
    EXPECT_EQ(params->dirMode(), 02775);
}
