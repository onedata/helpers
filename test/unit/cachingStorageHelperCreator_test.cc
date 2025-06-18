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
                std::move(storageHelperCreator), std::chrono::seconds{2});
    }

    void TearDown() override { m_executor->join(); }

protected:
    std::unordered_map<folly::fbstring, folly::fbstring> createDefaultArgs()
    {
        return {{"type", NULL_DEVICE_HELPER_NAME}, {"latencyMin", "0"},
            {"latencyMax", "0"}, {"timeoutProbability", "0"}};
    }

    MockCommunicator m_communicator;
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
    std::shared_ptr<NullDeviceHelperFactory> m_nullDeviceFactory;
    std::shared_ptr<CachingStorageHelperCreator<MockCommunicator>>
        m_cachingCreator;
};

TEST_F(CachingStorageHelperCreatorTest, ShouldReturnSameHelperForSameArgs)
{
    // Given
    auto args = createDefaultArgs();
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args, buffered);

    // Then
    ASSERT_EQ(helper1, helper2);
    ASSERT_EQ(helper1->id(), helper2->id());
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldReturnDifferentHelpersForDifferentArgs)
{
    // Given
    auto args1 = createDefaultArgs();
    auto args2 = createDefaultArgs();
    args2["latencyMin"] = "10";
    args2["latencyMax"] = "20";
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args1, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args2, buffered);

    // Then
    ASSERT_NE(helper1, helper2);
    ASSERT_NE(helper1->id(), helper2->id());
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldReturnDifferentHelpersForDifferentBufferedFlag)
{
    // Given
    auto args = createDefaultArgs();

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, true);
    auto helper2 = m_cachingCreator->getStorageHelper(args, false);

    // Then
    ASSERT_NE(helper1, helper2);
    ASSERT_NE(helper1->id(), helper2->id());
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldReturnSameHelperForSameArgsInDifferentOrder)
{
    // Given
    auto args1 = createDefaultArgs();
    auto args2 = std::unordered_map<folly::fbstring, folly::fbstring>{
        {"timeoutProbability", "0"}, {"latencyMax", "0"}, {"latencyMin", "0"},
        {"type", NULL_DEVICE_HELPER_NAME}};
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args1, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args2, buffered);

    // Then
    ASSERT_EQ(helper1, helper2);
    ASSERT_EQ(helper1->id(), helper2->id());
}

TEST_F(CachingStorageHelperCreatorTest, ShouldKeepHelperWhileReferenced)
{
    // Given
    auto args = createDefaultArgs();
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args, buffered);

    // Release one reference
    ASSERT_FALSE(m_cachingCreator->clean());

    // Get another reference - should return the same helper
    auto helper3 = m_cachingCreator->getStorageHelper(args, buffered);

    // Then
    ASSERT_EQ(helper1, helper2);
    ASSERT_EQ(helper1, helper3);
    ASSERT_EQ(helper1->id(), helper2->id());
    ASSERT_EQ(helper1->id(), helper3->id());
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldRemoveHelperWhenAllReferencesReleased)
{
    // Given
    auto args = createDefaultArgs();
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, buffered);
    const auto *helper1Ptr = helper1.get();
    const auto helper1Id = helper1->id();
    helper1.reset();

    // Clean stale cache entries
    std::this_thread::sleep_for(std::chrono::seconds{3});
    ASSERT_TRUE(m_cachingCreator->clean());

    // Get a new helper - should be different since the cache was cleared
    auto helper3 = m_cachingCreator->getStorageHelper(args, buffered);

    // Then
    ASSERT_NE(helper1Ptr, helper3.get());
    ASSERT_EQ(helper1Id, helper3->id());
}

TEST_F(CachingStorageHelperCreatorTest,
    ShouldHandleMultipleGetAndReleaseOperations)
{
    // Given
    auto args = createDefaultArgs();
    bool buffered = false;

    // When
    auto helper1 = m_cachingCreator->getStorageHelper(args, buffered);
    auto helper2 = m_cachingCreator->getStorageHelper(args, buffered);
    auto helper3 = m_cachingCreator->getStorageHelper(args, buffered);

    // Release in random order
    helper1.reset();
    helper2.reset();

    // Get another reference before final release
    auto helper4 = m_cachingCreator->getStorageHelper(args, buffered);

    ASSERT_EQ(helper3, helper4);
}