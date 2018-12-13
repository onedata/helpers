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

namespace one {
namespace helpers {

void init()
{
    LOG_FCALL();

    folly::SingletonVault::singleton()->registrationComplete();

    folly::ssl::init();
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
