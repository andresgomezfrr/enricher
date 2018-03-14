package zz.ks.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zz.ks.builder.bootstrap.ThreadBootstraper;
import zz.ks.builder.config.Config;
import zz.ks.exceptions.PlanBuilderException;
import zz.ks.metrics.MetricsManager;
import zz.ks.model.PlanModel;

import java.io.IOException;

import static zz.ks.builder.config.Config.ConfigProperties.BOOTSTRAPER_CLASSNAME;

public class Builder {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Config config;
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    ThreadBootstraper threadBootstraper;
    MetricsManager metricsManager;

    public Builder(Config config) throws Exception {
        this.config = config;
        metricsManager = new MetricsManager(config.clone());
        metricsManager.start();

        streamBuilder = new StreamBuilder(config.clone(), metricsManager);

        Class bootstraperClass = Class.forName(config.get(BOOTSTRAPER_CLASSNAME));
        threadBootstraper = (ThreadBootstraper) bootstraperClass.newInstance();
        threadBootstraper.init(this, config.clone(), metricsManager);
        threadBootstraper.start();

    }

    public void updateStreamConfig(String streamConfig) throws IOException, PlanBuilderException {
        if (streams != null) {
            metricsManager.clean();
            streamBuilder.close();
            streams.close();
            log.info("Clean Enricher process");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
        log.info("Execution plan: {}", model.printExecutionPlan());
        log.info("-------- TOPOLOGY BUILD START --------");
        KStreamBuilder builder = streamBuilder.builder(model);
        log.info("--------  TOPOLOGY BUILD END  --------");

        streams = new KafkaStreams(builder, config.getProperties());
        streams.setUncaughtExceptionHandler((thread, exception) -> {
            if (!exception.getMessage().contains("Topic not found")) {
                log.error(exception.getMessage(), exception);
            } else {
                log.warn("Creating topics, try execute Enricher again!");
            }

            metricsManager.interrupt();
            threadBootstraper.interrupt();
            streamBuilder.close();
        });
        streams.start();

        log.info("Started Enricher with conf {}", config.getProperties());
    }

    public void close() {
        metricsManager.interrupt();
        threadBootstraper.interrupt();
        streamBuilder.close();
        if (streams != null) streams.close();
    }
}
