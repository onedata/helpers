/**
 * @file flatOpScheduler.h
 * @author Konrad Zemek
 * @copyright (C) 2018 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */
#pragma once

#include <boost/variant/apply_visitor.hpp>
#include <folly/Executor.h>
#include <folly/FBVector.h>

#include <memory>
#include <mutex>

namespace one {
namespace helpers {

template <typename OpVariant, typename OpVisitor>
class FlatOpScheduler : public std::enable_shared_from_this<
                            FlatOpScheduler<OpVariant, OpVisitor>> {
public:
    using This = FlatOpScheduler<OpVariant, OpVisitor>;

    static std::shared_ptr<This> create(
        std::shared_ptr<folly::Executor> executor,
        std::shared_ptr<OpVisitor> opVisitor)
    {
        return std::shared_ptr<This>(new This{executor, opVisitor});
    }

    template <typename Op>
    auto schedule(Op &&op) -> decltype(op.promise.getFuture())
    {
        bool shouldIDrain = false;
        auto future = op.promise.getFuture();

        {
            std::lock_guard<std::mutex> guard{m_queueMutex};
            m_filledQueue.emplace_back(std::forward<Op>(op));
            shouldIDrain = !m_drainInProgress;
            m_drainInProgress = true;
        }

        if (shouldIDrain)
            m_executor->add(
                std::bind(&This::drainQueue, this->shared_from_this()));

        return future;
    }

    void drainQueue()
    {
        auto raii = m_opVisitor->startDrain();

        while (true) {
            assert(m_usedQueue.empty());
            {
                std::lock_guard<std::mutex> guard{m_queueMutex};
                if (m_filledQueue.empty()) {
                    m_drainInProgress = false;
                    return;
                }
                m_usedQueue.swap(m_filledQueue);
            }

            for (auto &op : m_usedQueue)
                boost::apply_visitor(*m_opVisitor, op);

            m_usedQueue.clear();
        }
    }

private:
    FlatOpScheduler(std::shared_ptr<folly::Executor> executor,
        std::shared_ptr<OpVisitor> opVisitor)
        : m_executor{std::move(executor)}
        , m_opVisitor{std::move(opVisitor)}
    {
    }

    std::shared_ptr<folly::Executor> m_executor;
    std::shared_ptr<OpVisitor> m_opVisitor;
    std::mutex m_queueMutex;
    bool m_drainInProgress = false;
    folly::fbvector<OpVariant> m_filledQueue;
    folly::fbvector<OpVariant> m_usedQueue;
};

} // namespace helpers
} // namespace one
