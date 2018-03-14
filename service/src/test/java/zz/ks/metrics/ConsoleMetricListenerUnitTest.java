package zz.ks.metrics;

import org.junit.Test;
import zz.ks.builder.config.Config;

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
                .put("metric.listeners", Collections.singletonList("zz.ks.metrics.ConsoleMetricListener"));

        MetricsManager metricsManager = new MetricsManager(config);

        assertFalse(metricsManager.listeners.isEmpty());

        assertTrue(metricsManager.listeners.get(0) instanceof ConsoleMetricListener);

    }

}
