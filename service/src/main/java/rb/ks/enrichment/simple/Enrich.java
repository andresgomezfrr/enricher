package rb.ks.enrichment.simple;

import rb.ks.metrics.MetricsManager;

import java.util.Map;

public interface Enrich {

    /**
     * Initialize enrich
     * @param properties Properties for enrich
     * @param metricsManager MetricsManager object for function
     */
    void init(Map<String, Object> properties, MetricsManager metricsManager);

    /**
     * Initialize enrich, this method is implemented by the users to for example:
     * initiate variables, load config, open DB connections.
     * @param properties Properties for enrich
     * @param metricsManager MetricsManager object for enrich
     */
    void prepare(Map<String, Object> properties, MetricsManager metricsManager);

    /**
     * Main logic of enrich
     * @param value The value of Kafka message
     * @return Returned value
     */
    Map<String, Object> enrich(Map<String, Object> value);

    /**
     * Stop enrich, this method is implemented by the users to for example: close DB connection
     */
    void stop();
}
