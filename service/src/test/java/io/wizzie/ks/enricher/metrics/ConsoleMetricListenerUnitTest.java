package io.wizzie.ks.enricher.metrics;

import io.wizzie.bootstrapper.builder.*;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.metrics.listeners.ConsoleMetricListener;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConsoleMetricListenerUnitTest {

    @Test
    public void metricManagerShouldHaveAConsoleMetricListener() {
        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metric.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("num.stream.threads", 1)
                .put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.ConsoleMetricListener"));

        MetricsManager metricsManager = new MetricsManager(config.getMapConf());

        assertFalse(metricsManager.listeners.isEmpty());

        assertTrue(metricsManager.listeners.get(0) instanceof ConsoleMetricListener);

    }

}
