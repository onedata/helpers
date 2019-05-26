/**
 * @file flatOpScheduler_test.cc
 * @author Bartek Kryza
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "asioExecutor.h"
#include "flatOpScheduler.h"
#include "testUtils.h"

#include <asio.hpp>
#include <boost/make_shared.hpp>
#include <boost/variant.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <folly/Executor.h>
#include <folly/ThreadName.h>
#include <folly/futures/Promise.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace ::testing;
using namespace one;
using namespace one::helpers;
using namespace one::testing;
using namespace std::chrono_literals;

struct PromiseOp {
    folly::Promise<folly::Unit> promise;
};

struct IncrementOp : public PromiseOp {
};

struct DecrementOp : public PromiseOp {
};

using HandleOp = boost::variant<IncrementOp, DecrementOp>;

using CounterType = std::atomic<int64_t>;

struct OpExec : public boost::static_visitor<> {
    OpExec(CounterType &counter)
        : m_counter{counter}
    {
    }

    std::unique_ptr<folly::Unit> startDrain()
    {
        return std::make_unique<folly::Unit>();
    }

    void operator()(IncrementOp &op) const
    {
        std::this_thread::sleep_for(500us);
        m_counter++;
        op.promise.setValue();
    }

    void operator()(DecrementOp &op) const
    {
        std::this_thread::sleep_for(250us);
        m_counter--;
        op.promise.setValue();
    }

    CounterType &m_counter;
};

struct FlatOpSchedulerTest : public ::testing::Test {
    FlatOpSchedulerTest()
        : m_executor{std::make_shared<AsioExecutor>(m_ioService)}
    {
    }

    ~FlatOpSchedulerTest() {}

    void SetUp() override
    {
        for (std::size_t i = 0; i < 50; i++) {
            m_poolWorkers.emplace_back([&, tid = i ] {
                folly::setThreadName("FOPS-" + std::to_string(tid));
                m_ioService.run();
            });
        }
    }

    void TearDown() override
    {
        m_ioService.stop();
        for (auto &t : m_poolWorkers)
            t.join();

        m_poolWorkers.clear();
    }

    asio::io_service m_ioService;
    asio::executor_work_guard<asio::io_service::executor_type> m_work{
        asio::make_work_guard(m_ioService)};
    std::vector<std::thread> m_poolWorkers;
    std::shared_ptr<AsioExecutor> m_executor;
};

TEST_F(FlatOpSchedulerTest, flatOpSchedulerShouldExecuteAllOps)
{
    CounterType counter;
    counter = 0;

    constexpr auto iterationCount = 1000;

    auto flatOpScheduler = FlatOpScheduler<HandleOp, OpExec>::create(
        m_executor, std::make_shared<OpExec>(counter));

    std::vector<folly::Future<folly::Unit>> futs;
    futs.reserve(3 * iterationCount);

    for (int i = 0; i < iterationCount; i++) {
        futs.emplace_back(flatOpScheduler->schedule(IncrementOp{}));
        futs.emplace_back(flatOpScheduler->schedule(DecrementOp{}));
        futs.emplace_back(flatOpScheduler->schedule(IncrementOp{}));
    }

    folly::collectAll(futs.begin(), futs.end()).get();

    EXPECT_EQ(counter, iterationCount);
}
