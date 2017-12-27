package io.wizzie.ks.enricher.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.ks.enricher.model.PlanModel;
import io.wizzie.ks.enricher.model.exceptions.PlanBuilderException;
import io.wizzie.ks.enricher.serializers.JsonSerde;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.wizzie.ks.enricher.base.builder.config.ConfigProperties.BOOTSTRAPER_CLASSNAME;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;


public class Builder implements Listener {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Config config;
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;

    public Builder(Config config) throws Exception {
        this.config = config;

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        metricsManager = new MetricsManager(config.clone().getMapConf());
        metricsManager.start();

        streamBuilder = new StreamBuilder(config.clone(), metricsManager);

        bootstrapper = BootstrapperBuilder.makeBuilder()
                .boostrapperClass(config.get(BOOTSTRAPER_CLASSNAME))
                .listener(this)
                .withConfigInstance(config)
                .build();
    }

    public void close() throws Exception {
        metricsManager.interrupt();
        streamBuilder.close();
        bootstrapper.close();
        if (streams != null) streams.close();
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String streamConfig) {
        if (streams != null) {
            metricsManager.clean();
            streamBuilder.close();
            streams.close();
            log.info("Clean Enricher process");
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
            log.info("Execution plan: {}", model.printExecutionPlan());
            log.info("-------- TOPOLOGY BUILD START --------");
            StreamsBuilder builder = streamBuilder.builder(model);
            log.info("--------  TOPOLOGY BUILD END  --------");

            Config configWithNewAppId = config.clone();
            String appId = configWithNewAppId.get(APPLICATION_ID_CONFIG);
            configWithNewAppId.put(APPLICATION_ID_CONFIG, String.format("%s_%s", appId, "enricher"));
            streams = new KafkaStreams(builder.build(), configWithNewAppId.getProperties());
            streams.setUncaughtExceptionHandler((thread, exception) -> log.error(exception.getMessage(), exception));
            streams.start();

            log.info("Started Enricher with conf {}", config.getProperties());
        } catch (PlanBuilderException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
