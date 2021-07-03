/**
 * @file scheduler.h
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#ifndef HELPERS_SCHEDULER_H
#define HELPERS_SCHEDULER_H

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

namespace one {

/**
 * The Scheduler class is responsible for scheduling work to an underlying pool
 * of worker threads.
 */
class Scheduler {
public:
    /**
     * Constructor.
     * Creates worker threads.
     * @param threadNumber The number of threads to be spawned.
     */
    Scheduler(const int threadNumber);

    /**
     * Destructor.
     * Stops the scheduler and joins worker threads.
     */
    virtual ~Scheduler() = default;

    void prepareForDaemonize();
    void restartAfterDaemonize();

    /**
     * Runs a task asynchronously in @c Scheduler's thread pool.
     * @param task The task to execute.
     */
    template <typename F> void post(F &&task)
    {
        m_executor->add(std::forward<F>(task));
    }

    /**
     * Runs a task asynchronously in @c Scheduler's thread pool on an object
     * referenced by a non-owning pointer.
     * @param member The member to invoke.
     * @param subject The subject whose member is to be invoked.
     * @param args Arguments to pass to the member.
     */
    template <class R, class T, class... Args>
    void post(R(T::*member), std::weak_ptr<T> subject, Args &&... args)
    {
        auto task = std::bind(
            member, std::placeholders::_1, std::forward<Args>(args)...);

        post([subject = std::move(subject), task = std::move(task)] {
            if (auto s = subject.lock())
                task(s.get());
        });
    }

    /**
     * A convenience overload for @c post taking a @c std::shared_ptr.
     */
    template <class R, class T, class... Args>
    void post(R(T::*member), const std::shared_ptr<T> &subject, Args &&... args)
    {
        post(member, std::weak_ptr<T>{subject}, std::forward<Args>(args)...);
    }

    /**
     * Schedules a task to be run after some time.
     * @param after The duration after which the task should be executed.
     * @param task The task to execute.
     * @return A function to cancel the scheduled task.
     */
    template <typename Rep, typename Period, typename F>
    std::function<void()> schedule(
        const std::chrono::duration<Rep, Period> after, F &&task)
    {
        auto f = std::make_shared<folly::Future<folly::Unit>>(
            folly::via(m_executor.get())
                .delayed(after)
                .then(
                    [e = std::weak_ptr<folly::IOThreadPoolExecutor>(m_executor),
                        t = std::forward<F>(task)]() {
                        if (auto executor = e.lock()) {
                            t();
                        }
                    }));

        return [f = std::move(f)]() mutable {
            try {
                f->cancel();
            }
            catch (folly::FutureCancellation &) {
            }
        };
    }

    /**
     * Schedules a task to be run after some time on an object referenced by a
     * non-owning pointer.
     * @param after The duration after which the task should be executed.
     * @param member The member to invoke.
     * @param subject The subject whose member is to be invoked.
     * @param args Arguments to pass to the member.
     * @return A function to cancel the scheduled task.
     */
    template <typename Rep, typename Period, class R, class T, class... Args>
    std::function<void()> schedule(
        const std::chrono::duration<Rep, Period> after, R(T::*member),
        std::weak_ptr<T> subject, Args &&... args)
    {
        auto task = std::bind(
            member, std::placeholders::_1, std::forward<Args>(args)...);

        return schedule(
            after, [subject = std::move(subject), task = std::move(task)] {
                if (auto s = subject.lock())
                    task(s.get());
            });
    }

    /**
     * A convenience overload for @c schedule taking a @c std::shared_ptr.
     */
    template <typename Rep, typename Period, class R, class T, class... Args>
    std::function<void()> schedule(
        const std::chrono::duration<Rep, Period> after, R(T::*member),
        const std::shared_ptr<T> &subject, Args &&... args)
    {
        return schedule(after, member, std::weak_ptr<T>{subject},
            std::forward<Args>(args)...);
    }

private:
    const int m_threadNumber;
    std::shared_ptr<folly::IOThreadPoolExecutor> m_executor;
};

} // namespace one

#endif // HELPERS_SCHEDULER_H
