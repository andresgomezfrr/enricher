package rb.ks.builder;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.config.Config;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.metrics.MetricsManager;
import rb.ks.model.PlanModel;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    private Map<String, KStream<String, Map<String, Object>>> kStreams = new HashMap<>();

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate();

        clean();

        KStreamBuilder builder = new KStreamBuilder();

        return builder;
    }


    private void clean() {
        kStreams.clear();
    }

    /*
    TODO: Refactor to makeJoiners
    private Function makeFunction(String className, Map<String, Object> properties)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class funcClass = Class.forName(className);
        Function func = (Function) funcClass.newInstance();
        func.init(properties, metricsManager);
        return func;
    }
    */
}
