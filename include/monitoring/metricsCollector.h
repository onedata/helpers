/**
 * @file metricsCollector.h
 * @author Bartek Kryza
 * @copyright (C) 2017 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in
 * 'LICENSE.txt'
 */

#pragma once

#include "cppmetrics/cppmetrics.h"
#include "monitoring/monitoringConfiguration.h"

namespace one {
namespace monitoring {

using namespace cppmetrics;
/**
 * MetricsCollector is an abstract class responsible for managing performance
 * metrics collection in 'helpers' and 'oneclient' and reporting them to
 * Graphite or console.
 */
class MetricsCollector {
public:
    /**
     * Default constructor.
     * @param conf Monitoring configuration options.
     */
    MetricsCollector();

    /**
     * Destructor.
     */
    virtual ~MetricsCollector();

    MetricsCollector(const MetricsCollector &) = delete;
    MetricsCollector(MetricsCollector &&) = delete;
    MetricsCollector &operator=(const MetricsCollector &) = delete;
    MetricsCollector &operator=(MetricsCollector &&) = delete;

    /**
     * Configure the monitoring metrics collector.
     * @param monitoringConfiguration New configuration.
     */
    void setConfiguration(
        std::shared_ptr<MonitoringConfiguration> monitoringConfiguration);

    /**
     * Return the current monitoring configuration.
     */
    const MonitoringConfiguration &getConfiguration() const;

    /**
     * Initialize the metrics collector. It has to be configured first.
     */
    virtual void initialize();

    /**
     * Start the metrics collector and reporting process.
     */
    virtual void start();

    /**
     * Stop the metrics collector and reporting process.
     */
    virtual void stop();

    /**
     * Get the underlying MetricRegistry instance.
     */
    static core::MetricRegistryPtr getRegistry();

    /**
     * Return the MetricsCollector singleton instance
     */
    template <typename TMetricsCollector = MetricsCollector>
    static std::shared_ptr<MetricsCollector> getInstance()
    {
        if (!m_singleton) {
            m_singleton = std::dynamic_pointer_cast<MetricsCollector>(
                std::make_shared<TMetricsCollector>());
            m_isEnabled = true;
        }

        return m_singleton;
    }

    /**
     * Check if the Metrics collector is enabled.
     */
    static bool isEnabled() { return m_isEnabled; }

protected:
    std::shared_ptr<MonitoringConfiguration> m_conf;                 // NOLINT
    std::shared_ptr<cppmetrics::core::ScheduledReporter> m_reporter; // NOLINT

private:
    static std::shared_ptr<MetricsCollector> m_singleton;
    static bool m_isEnabled;
};

} // namespace monitoring
} // namespace one
