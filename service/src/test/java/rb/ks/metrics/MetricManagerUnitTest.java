package rb.ks.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.junit.Test;
import rb.ks.builder.config.Config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class MetricManagerUnitTest {


    @Test
    public void metricManagerNotRunningIfConfigIsEmpty() {

        Config emptyConfig = new Config();
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
                .put("num.stream.threads", 1)
                .put("metric.listeners", Collections.singletonList("rb.ks.metrics.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config);

        assertTrue(metricsManager.config.get("metric.enable"));

        assertEquals(new Long(2000), metricsManager.interval);
        assertEquals("testing-metric-manager", metricsManager.app_id);
        assertEquals(new Integer(1), metricsManager.num_threads);

    }

    @Test
    public void metricManagerShouldRegisterMetrics() {
        Config config = new Config();

        config
                .put("metric.enable", true)
                .put("metrinc.interval", 2000)
                .put("application.id", "testing-metric-manager")
                .put("num.stream.threads", 1)
                .put("metric.listeners", Collections.singletonList("rb.ks.metrics.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config);

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
                .put("metric.listeners", Collections.singletonList("rb.ks.metrics.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config);

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
                .put("metric.listeners", Collections.singletonList("rb.ks.metrics.ConsoleMetricListener"));


        MetricsManager metricsManager = new MetricsManager(config);

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
