package io.wizzie.ks.enricher.enrichment.simple;

import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class BaseEnrich implements Enrich, ValueMapper<Map<String, Object>, Map<String, Object>> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void init(Map<String, Object> properties, MetricsManager metricsManager) {
        prepare(properties, metricsManager);
        log.info("   with {}", toString());
    }

    @Override
    public Map<String, Object> apply(Map<String, Object> value) {
        return enrich(value);
    }
}
