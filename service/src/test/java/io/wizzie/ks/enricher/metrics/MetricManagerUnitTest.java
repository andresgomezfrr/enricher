package io.wizzie.ks.enricher.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.metrics.MetricsManager;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class MetricManagerUnitTest {


    @Test
    public void metricManagerNotRunningIfConfigIsEmpty() {

        Map<String, Object> emptyConfig = new HashMap<>();
        MetricsManager metricsManager = new MetricsManager(emptyConfig);

        assertFalse(metricsManager.running.get());
    }

    @Test
    public void metricManagerShouldLoadConfig() {
        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metric.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config.getMapConf());

        assertTrue((Boolean) metricsManager.config.get("metric.enable"));

        assertEquals(new Long(2000), metricsManager.interval);
        assertEquals("testing-metric-manager", metricsManager.app_id);

    }

    @Test
    public void metricManagerShouldRegisterMetrics() {
        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metrinc.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config.getMapConf());

        assertEquals(0, metricsManager.registredMetrics.size());

        metricsManager.registerMetric("myCounterMetric", new Counter());
        metricsManager.registerMetric("myTimerMetric", new Timer());

        assertEquals(2, metricsManager.registredMetrics.size());

        Set<String> expectedMetrics = new HashSet<>();
        expectedMetrics.add("myCounterMetric");
        expectedMetrics.add("myTimerMetric");

        assertEquals(expectedMetrics, metricsManager.registredMetrics);
    }

    @Test
    public void metricManagerShouldRemoveMetrics() {
        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metrinc.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("num.stream.threads", 1)
                .put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config.getMapConf());

        assertEquals(0, metricsManager.registredMetrics.size());

        metricsManager.registerMetric("myCounterMetric", new Counter());
        metricsManager.registerMetric("myTimerMetric", new Timer());

        assertEquals(2, metricsManager.registredMetrics.size());

        metricsManager.removeMetric("myCounterMetric");

        assertEquals(1, metricsManager.registredMetrics.size());

        Set<String> expectedMetrics = new HashSet<>();
        expectedMetrics.add("myTimerMetric");

        assertEquals(expectedMetrics, metricsManager.registredMetrics);
    }

    @Test
    public void metricManagerShouldCleanMetrics() {

        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metrinc.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("num.stream.threads", 1)
                .put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config.getMapConf());

        assertEquals(0, metricsManager.registredMetrics.size());

        metricsManager.registerMetric("myCounterMetric", new Counter());
        metricsManager.registerMetric("myTimerMetric", new Timer());

        assertEquals(2, metricsManager.registredMetrics.size());

        metricsManager.clean();

        assertEquals(0, metricsManager.registredMetrics.size());

        Set<String> expectedMetrics = new HashSet<>();

        assertEquals(expectedMetrics, metricsManager.registredMetrics);
    }

}
