/**
 * @file init.cc
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include "helpers/init.h"
#include "helpers/logging.h"
#include "monitoring/graphiteMetricsCollector.h"
#include "monitoring/metricsCollector.h"
#include "monitoring/monitoring.h"
#include "monitoring/monitoringConfiguration.h"
#include <folly/Singleton.h>
#include <folly/ssl/Init.h>

#include <array>

namespace one {
namespace helpers {

void init()
{
    LOG_FCALL();

    folly::SingletonVault::singleton()->registrationComplete();

    folly::ssl::init();
}

void startReadWritePerfLogger(const std::string &logDirectory)
{
    using one::logging::csv::read_write_perf;
    using one::logging::csv::register_logger;

    // Register object helper performance logger
    register_logger<read_write_perf>(logDirectory);
    spdlog::get("read_write_perf")->set_level(spdlog::level::info);

    // Set log levels based on spdlog compatible argv string
    spdlog::flush_every(std::chrono::seconds(4));
}

void configureMonitoring(
    std::shared_ptr<monitoring::MonitoringConfiguration> conf, bool start)
{
    LOG_FCALL() << LOG_FARG(start);

    std::shared_ptr<monitoring::MetricsCollector> metricsCollector;

    if (dynamic_cast<monitoring::GraphiteMonitoringConfiguration *>(
            conf.get()) != nullptr) {
        metricsCollector = monitoring::MetricsCollector::getInstance<
            monitoring::GraphiteMetricsCollector>();
    }
    else {
        LOG(ERROR) << "Unsupported monitoring type requested.";
        throw std::runtime_error("Unsupported monitoring type requested.");
    }

    metricsCollector->setConfiguration(std::move(conf));

    if (start)
        metricsCollector->start();
}
} // namespace helpers
} // namespace one
