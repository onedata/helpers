/**
 * @file cachingStorageHelperCreator_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2025 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#undef BUILD_PROXY_IO

#include "helpers/cachingStorageHelperCreator.h"
#include "nullDeviceHelper.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include <memory>

using namespace ::testing;
using namespace one::helpers;

class MockCommunicator {
public:
    MockCommunicator() = default;
    ~MockCommunicator() = default;

    std::shared_ptr<folly::Executor> executor() { return {}; }
};

class CachingStorageHelperCreatorTest : public ::testing::Test {
public:
    void SetUp() override
    {
        m_executor = std::make_shared<folly::IOThreadPoolExecutor>(1);
        m_nullDeviceFactory =
            std::make_shared<NullDeviceHelperFactory>(m_executor);
        auto storageHelperCreator =
            std::make_unique<StorageHelperCreator<MockCommunicator>>(m_executor,
                m_executor, m_executor, m_executor, m_executor, m_executor,
                m_executor, m_executor, m_executor, m_executor);
        m_cachingCreator =
            std::make_shared<CachingStorageHelperCreator<MockCommunicator>>(
                std::move(storageHelperCreator));
    }

    void TearDown() override { m_executor->join(); }

protected:
    MockCommunicator m_communicator;
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    std::shared_ptr<NullDeviceHelperFactory> m_nullDeviceFactory;
    std::shared_ptr<CachingStorageHelperCreator<MockCommunicator>>
        m_cachingCreator;
};

TEST_F(CachingStorageHelperCreatorTest, ShouldReturnSameHelperForSameArgs)
{
    // Given
    std::unordered_map<folly::fbstring, folly::fbstring> args{
        {"type", NULL_DEVICE_HELPER_NAME}, {"latencyMin", "0"},
        {"latencyMax", "0"}, {"timeoutProbability", "0"}};
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args, buffered);

    // Then
    ASSERT_EQ(helper1, helper2);
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldReturnDifferentHelpersForDifferentArgs)
{
    // Given
    std::unordered_map<folly::fbstring, folly::fbstring> args1{
        {"type", NULL_DEVICE_HELPER_NAME}, {"latencyMin", "0"},
        {"latencyMax", "0"}, {"timeoutProbability", "0"}};

    std::unordered_map<folly::fbstring, folly::fbstring> args2{
        {"type", NULL_DEVICE_HELPER_NAME}, {"latencyMin", "10"},
        {"latencyMax", "20"}, {"timeoutProbability", "0"}};

    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args1, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args2, buffered);

    // Then
    ASSERT_NE(helper1, helper2);
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldReturnDifferentHelpersForDifferentBufferedFlag)
{
    // Given
    std::unordered_map<folly::fbstring, folly::fbstring> args{
        {"type", NULL_DEVICE_HELPER_NAME}, {"latencyMin", "0"},
        {"latencyMax", "0"}, {"timeoutProbability", "0"}};

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, true);
    auto helper2 = m_cachingCreator->getStorageHelper(args, false);

    // Then
    ASSERT_NE(helper1, helper2);
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldReturnSameHelperForSameArgsInDifferentOrder)
{
    // Given
    std::unordered_map<folly::fbstring, folly::fbstring> args1{
        {"type", NULL_DEVICE_HELPER_NAME}, {"latencyMin", "0"},
        {"latencyMax", "0"}, {"timeoutProbability", "0"}};

    std::unordered_map<folly::fbstring, folly::fbstring> args2{
        {"timeoutProbability", "0"}, {"latencyMax", "0"}, {"latencyMin", "0"},
        {"type", NULL_DEVICE_HELPER_NAME}};

    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args1, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args2, buffered);

    // Then
    ASSERT_EQ(helper1, helper2);
}