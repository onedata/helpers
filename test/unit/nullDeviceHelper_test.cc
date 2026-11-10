/**
 * @file nullDeviceHelper_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "nullDeviceHelper.h"
#include "testUtils.h"

#include <boost/make_shared.hpp>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ManualExecutor.h>
#include <gtest/gtest.h>

#include <tuple>

#include <boost/algorithm/string.hpp>
#include <folly/String.h>

using namespace ::testing;
using namespace one;
using namespace one::helpers;
using namespace one::testing;

struct NullDeviceHelperTest : public ::testing::Test {
    NullDeviceHelperTest() { }

    ~NullDeviceHelperTest() { m_ioExecutor->join(); }

    void SetUp() override { }

    void TearDown() override { }

    auto createHelper(const Params &params)
    {
        return std::dynamic_pointer_cast<NullDeviceHelper>(
            m_factory.createStorageHelper(params, ExecutionContext::ONECLIENT));
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> m_ioExecutor =
        std::make_shared<folly::IOThreadPoolExecutor>(
            1, std::make_shared<StorageWorkerFactory>("null_t"));

    NullDeviceHelperFactory m_factory{m_ioExecutor};

    std::shared_ptr<folly::ManualExecutor> m_executor{
        std::make_shared<folly::ManualExecutor>()};
};

TEST_F(NullDeviceHelperTest, timeoutWithZeroProbabilityShouldAlwaysBeFalse)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    for (int i = 0; i < 1000; i++)
        EXPECT_FALSE(helper->randomTimeout());
}

TEST_F(NullDeviceHelperTest, timeoutWithOneProbabilityShouldAlwaysBeTrue)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    for (int i = 0; i < 1000; i++)
        EXPECT_TRUE(helper->randomTimeout());
}

TEST_F(NullDeviceHelperTest, latencyShouldBeAlwaysInDefinedRange)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "250");
    p1.emplace("latencyMax", "750");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    for (int i = 0; i < 1000; i++) {
        auto latency = helper->randomLatency();
        EXPECT_TRUE(latency > 100);
        EXPECT_TRUE(latency < 1000);
    }
}

TEST_F(NullDeviceHelperTest, latencyWithZeroRangeShouldBeAlwaysReturnZero)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    for (int i = 0; i < 1000; i++)
        EXPECT_EQ(helper->randomLatency(), 0);
}

TEST_F(NullDeviceHelperTest, emptyFilterShouldAllowAnyOperation)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "100");
    p1.emplace("latencyMax", "1000");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    EXPECT_TRUE(helper->applies("whatever"));
}

TEST_F(NullDeviceHelperTest, wildcardFilterShouldAllowAnyOperation)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "100");
    p1.emplace("latencyMax", "1000");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    EXPECT_TRUE(helper->applies("whatever"));
}

TEST_F(NullDeviceHelperTest, singleWordFileterShouldAllowOnlyOneOperation)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "100");
    p1.emplace("latencyMax", "1000");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "truncate");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    EXPECT_TRUE(std::dynamic_pointer_cast<NullDeviceHelperParams>(
                    helper->params().get())
                    ->filter()
                    .size() == 1);

    EXPECT_TRUE(std::dynamic_pointer_cast<NullDeviceHelperParams>(
                    helper->params().get())
                    ->filter()
                    .at(0) == "truncate");

    EXPECT_FALSE(std::dynamic_pointer_cast<NullDeviceHelperParams>(
        helper->params().get())
                     ->applyToAllOperations());

    EXPECT_FALSE(helper->applies("whatever"));
    EXPECT_FALSE(helper->applies(""));
    EXPECT_TRUE(helper->applies("truncate"));
}

TEST_F(NullDeviceHelperTest, multipleOpsShouldAllowOnlyTheseOperations)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "100");
    p1.emplace("latencyMax", "1000");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "truncate,read,write");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);
    EXPECT_FALSE(helper->applies("whatever"));
    EXPECT_FALSE(helper->applies(""));
    EXPECT_TRUE(helper->applies("truncate"));
    EXPECT_TRUE(helper->applies("read"));
    EXPECT_TRUE(helper->applies("write"));
}

TEST_F(
    NullDeviceHelperTest, multipleOpsWithSpacesShouldAllowOnlyTheseOperations)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "100");
    p1.emplace("latencyMax", "1000");
    p1.emplace("timeoutProbability", "1.0");
    p1.emplace("filter", "\t\t\ntruncate,\nread,\n   write  ");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    EXPECT_FALSE(helper->applies("whatever"));
    EXPECT_FALSE(helper->applies(""));
    EXPECT_TRUE(helper->applies("truncate"));
    EXPECT_TRUE(helper->applies("read"));
    EXPECT_TRUE(helper->applies("write"));
}

TEST_F(NullDeviceHelperTest, readReturnsRequestedNumberOfBytes)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");

    auto helper = createHelper(p1);

    auto handle = helper->open("whatever", O_RDWR, {}).getVia(m_executor.get());

    EXPECT_EQ(
        handle->read(1000, 100).getVia(m_executor.get()).chainLength(), 100);
}

TEST_F(
    NullDeviceHelperTest, readWriteDataIsConsistentWhenEnabledDataVerification)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");
    p1.emplace("enableDataVerification", "true");

    auto helper = createHelper(p1);

    auto handle = helper->open("whatever", O_RDWR, {}).getVia(m_executor.get());

    {
        std::string result{};
        handle->read(0, 4).getVia(m_executor.get()).appendToString(result);

        EXPECT_EQ(result, "abcd");

        folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
        writeBuf.append(result);
        handle->write(0, std::move(writeBuf), {}).getVia(m_executor.get());
    }
    {
        std::string result{};
        handle->read(64, 4).getVia(m_executor.get()).appendToString(result);

        EXPECT_EQ(result, "abcd");

        folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
        writeBuf.append(result);
        handle->write(64, std::move(writeBuf), {}).getVia(m_executor.get());
    }
    {
        std::string result{};
        handle->read(64 + 1, 4).getVia(m_executor.get()).appendToString(result);

        EXPECT_EQ(result, "bcde");

        folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
        writeBuf.append(result);
        handle->write(1, std::move(writeBuf), {}).getVia(m_executor.get());
    }
    {
        std::string result{};
        handle->read(0, 160 * 1024 * 1024)
            .getVia(m_executor.get())
            .appendToString(result);

        EXPECT_EQ(result[0], 'a');
        EXPECT_EQ(result[64], 'a');
        EXPECT_EQ(result[10000 * 64], 'a');
        EXPECT_EQ(result.back(), '=');
        EXPECT_EQ(result.substr(10000 * 64, 64),
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
            "+=");

        folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
        writeBuf.append(result);
        handle->write(0, std::move(writeBuf), {}).getVia(m_executor.get());
    }
}

TEST_F(NullDeviceHelperTest, writeReturnsWrittenNumberOfBytes)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");
    p1.emplace("enableDataVerification", "false");

    auto helper = createHelper(p1);

    auto handle = helper->open("whatever", O_RDWR, {}).getVia(m_executor.get());

    std::size_t size = 10 * 1024 * 1024;
    std::string stmp(size, 'y');
    std::string tmp;

    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    writeBuf.append(stmp);

    auto bytesWritten =
        handle->write(0, std::move(writeBuf), {}).getVia(m_executor.get());

    EXPECT_EQ(bytesWritten, size);
}

TEST_F(NullDeviceHelperTest, readTimesAreInLatencyBoundaries)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "25");
    p1.emplace("latencyMax", "75");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");
    p1.emplace("enableDataVerification", "false");

    auto helper = createHelper(p1);

    auto handle = helper->open("whatever", O_RDWR, {}).getVia(m_executor.get());

    for (int i = 0; i < 50; i++) {
        auto start = std::chrono::steady_clock::now();

        handle->read(1000, 100).getVia(m_executor.get());

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

        EXPECT_TRUE(duration.count() >= 25);
        EXPECT_TRUE(duration.count() <= 75 + 10);
    }
}

TEST_F(NullDeviceHelperTest,
    nullHelperParamsShouldParseSimulatedFilesystemParameters)
{
    auto empty = "";

    auto simulatedFilesystemParams =
        NullDeviceHelperParams::parseSimulatedFilesystemParameters(empty);
    EXPECT_TRUE(std::get<0>(simulatedFilesystemParams).empty());
    EXPECT_FALSE(std::get<1>(simulatedFilesystemParams));

    auto oneLevelStr = "10-0";
    auto oneLevel =
        std::vector<std::pair<long int, long int>>{std::make_pair(10, 0)};
    auto oneLevelResult =
        std::get<0>(NullDeviceHelperParams::parseSimulatedFilesystemParameters(
            oneLevelStr));

    EXPECT_EQ(std::get<0>(oneLevel[0]), 10);
    EXPECT_EQ(std::get<1>(oneLevel[0]), 0);

    auto twoLevelsStr = "10-5:1-20";
    auto twoLevels =
        std::get<0>(NullDeviceHelperParams::parseSimulatedFilesystemParameters(
            twoLevelsStr));

    EXPECT_EQ(std::get<0>(twoLevels[0]), 10);
    EXPECT_EQ(std::get<1>(twoLevels[0]), 5);
    EXPECT_EQ(std::get<0>(twoLevels[1]), 1);
    EXPECT_EQ(std::get<1>(twoLevels[1]), 20);

    auto invalidFormat = "10:1-20";
    EXPECT_THROW(NullDeviceHelperParams::parseSimulatedFilesystemParameters(
                     invalidFormat),
        std::invalid_argument);

    auto invalidNumbers = "a10-1";
    EXPECT_THROW(NullDeviceHelperParams::parseSimulatedFilesystemParameters(
                     invalidNumbers),
        std::invalid_argument);
}

TEST_F(NullDeviceHelperTest, simulatedFilesystemEntryCountShouldWork)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace("simulatedFilesystemParameters", "2-2:2-2:0-3");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");
    p1.emplace("enableDataVerification", "false");

    auto helper = createHelper(p1);

    EXPECT_EQ(helper->simulatedFilesystemLevelEntryCount(0), 2 + 2);
    EXPECT_EQ(helper->simulatedFilesystemLevelEntryCount(1), 2 * (2 + 2));
    EXPECT_EQ(helper->simulatedFilesystemLevelEntryCount(2), 2 * 2 * (0 + 3));

    EXPECT_EQ(helper->simulatedFilesystemEntryCount(), 4 + 8 + 12);
}

TEST_F(NullDeviceHelperTest, simulatedFilesystemFileDistShouldWork)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace(
        "simulatedFilesystemParameters", "10-20:10-20:8-1:0-100:1000000000");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");
    p1.emplace("enableDataVerification", "false");

    auto helper = createHelper(p1);

    EXPECT_EQ(helper->simulatedFilesystemFileDist({"1"}), 1);
    EXPECT_EQ(helper->simulatedFilesystemFileDist({"25"}), 25);
    EXPECT_EQ(helper->simulatedFilesystemFileDist({"1", "1"}), 31);
    EXPECT_EQ(helper->simulatedFileSize(), 1'000'000'000);
}

TEST_F(NullDeviceHelperTest,
    simulatedFilesystemFileDistShouldWorkFromMultipleThreads)
{
    Params p1;
    p1.emplace("type", "nulldevice");
    p1.emplace("name", "someNullDevice");
    p1.emplace("latencyMin", "0");
    p1.emplace("latencyMax", "0");
    p1.emplace("timeoutProbability", "0.0");
    p1.emplace("filter", "*");
    p1.emplace(
        "simulatedFilesystemParameters", "10000-0:10000-10000:0-10000:1024");
    p1.emplace("simulatedFilesystemGrowSpeed", "0.0");
    p1.emplace("enableDataVerification", "false");

    auto helper = createHelper(p1);

    auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(50);

    std::vector<folly::Future<struct stat>> futs;
    std::vector<folly::Future<folly::fbvector<folly::fbstring>>> futs2;

    for (int di = 0; di < 2000; di++) {
        futs.emplace_back(folly::via(executor.get(), [&helper, di]() {
            return helper->getattr(fmt::format("/{}", di));
        }));
    }

    for (int di = 0; di < 1000; di++) {
        futs2.emplace_back(folly::via(executor.get(), [&helper, di]() {
            return helper->readdir(fmt::format("/{}", di), 0, 2000);
        }));
        for (int fi = 10000; fi < 10000 + 100; fi++) {
            futs.emplace_back(folly::via(executor.get(), [&helper, di, fi]() {
                return helper->getattr(
                    fmt::format("/{}/{}/{}/{}", di, fi, fi, fi));
            }));
        }
    }

    auto dir_results = folly::collectAll(futs2).via(executor.get()).get();
    auto file_results = folly::collectAll(futs).via(executor.get()).get();

    EXPECT_EQ(dir_results.size(), 1000);
    EXPECT_EQ(file_results.size(), 102000);

    executor->join();
}