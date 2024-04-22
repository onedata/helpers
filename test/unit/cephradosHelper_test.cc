/**
 * @file cephradosHelper_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "cephRadosHelper.h"
#include "testUtils.h"

#include <boost/make_shared.hpp>
#include <gtest/gtest.h>

#include <tuple>

using namespace ::testing;
using namespace one;
using namespace one::helpers;
using namespace one::testing;

struct CephRadosHelperTest : public ::testing::Test {
    CephRadosHelperTest() { }

    ~CephRadosHelperTest() { }

    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(CephRadosHelperTest, testDefaultHelperParams)
{
    using namespace boost::filesystem;

    Params params;
    params["clusterName"] = "test";
    params["monitorHostname"] = "localhost";
    params["poolName"] = "test";
    params["username"] = "test";
    params["key"] = "test";

    auto helper = std::make_unique<CephRadosHelper>(
        CephRadosHelperParams::create(params));

    ASSERT_EQ(helper->blockSize(), 10 * 1024 * 1024);
}
