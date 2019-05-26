/**
 * @file graphiteMetricsCollector.cc
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#include <memory>

#include "cppmetrics/cppmetrics.h"
#include "helpers/logging.h"
#include "monitoring/graphiteMetricsCollector.h"
#include "monitoring/monitoringConfiguration.h"

namespace one {
namespace monitoring {

GraphiteMetricsCollector::GraphiteMetricsCollector() = default;

GraphiteMetricsCollector::~GraphiteMetricsCollector() = default;

void GraphiteMetricsCollector::initialize()
{
    LOG_FCALL();

    auto conf =
        std::dynamic_pointer_cast<GraphiteMonitoringConfiguration>(m_conf);

    LOG_DBG(1) << "Initializing Graphite metrics reporter";

    if (!conf) {
        throw std::runtime_error("Invalid monitoring configuration type");
    }

    if (conf->graphiteProtocol ==
        GraphiteMonitoringConfiguration::GraphiteProtocol::TCP) {
        LOG_DBG(1) << "Creating TCP Graphite reporter to "
                   << conf->graphiteHostname << ":" << conf->graphitePort;
        m_sender = std::make_shared<graphite::GraphiteSenderTCP>(
            conf->graphiteHostname, conf->graphitePort);
    }
    else {
        LOG_DBG(1) << "Creating UDP Graphite reporter to "
                   << conf->graphiteHostname << ":" << conf->graphitePort;
        m_sender = std::make_shared<graphite::GraphiteSenderUDP>(
            conf->graphiteHostname, conf->graphitePort);
    }

    m_reporter = std::make_shared<graphite::GraphiteReporter>(
        getRegistry(), m_sender, conf->namespacePrefix);

    m_reporter->setReportingLevel(conf->reportingLevel);
}
} // namespace monitoring
} // namespace one
