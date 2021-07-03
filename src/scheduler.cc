/**
 * @file scheduler.cc
 * @author Konrad Zemek
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "scheduler.h"
#include "helpers/logging.h"
#include "helpers/storageHelper.h"

#include <folly/ThreadName.h>

namespace one {

Scheduler::Scheduler(const int threadNumber)
    : m_executor{std::make_shared<folly::IOThreadPoolExecutor>(threadNumber,
          std::make_shared<one::helpers::StorageWorkerFactory>("sched_t"))}
{
}

void Scheduler::prepareForDaemonize() { LOG_FCALL(); }

void Scheduler::restartAfterDaemonize() { LOG_FCALL(); }
} // namespace one
