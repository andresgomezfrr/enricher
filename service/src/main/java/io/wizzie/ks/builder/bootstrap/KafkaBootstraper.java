package io.wizzie.ks.builder.bootstrap;

import io.wizzie.ks.builder.Builder;
import io.wizzie.ks.builder.config.Config;
import io.wizzie.ks.exceptions.PlanBuilderException;
import io.wizzie.ks.metrics.MetricsManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class KafkaBootstraper extends ThreadBootstraper {
    private static final Logger log = LoggerFactory.getLogger(KafkaBootstraper.class);
    public final static String BOOTSTRAP_TOPIC = "__enricher_bootstrap";
    AtomicBoolean closed = new AtomicBoolean(false);
    TopicPartition storePartition = new TopicPartition(BOOTSTRAP_TOPIC, 0);
    public String appId;
    KafkaConsumer<String, String> restoreConsumer;
    Builder builder;

    @Override
    public void init(Builder builder, Config config, MetricsManager metricsManager) throws IOException, PlanBuilderException {
        this.builder = builder;
        Properties consumerConfig = new Properties();
        appId = config.get(APPLICATION_ID_CONFIG);
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, config.get(BOOTSTRAP_SERVERS_CONFIG));
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(GROUP_ID_CONFIG, String.format(
                "enricher-bootstraper-%s-%s", appId, UUID.randomUUID().toString())
        );

        restoreConsumer = new KafkaConsumer<>(consumerConfig);
        restoreConsumer.assign(Collections.singletonList(storePartition));

        // calculate the end offset of the partition
        // TODO: this is a bit hacky to first seek then position to get the end offset
        restoreConsumer.seekToEnd(singleton(storePartition));
        long endOffset = restoreConsumer.position(storePartition);

        // restore the state from the beginning of the change log otherwise
        restoreConsumer.seekToBeginning(singleton(storePartition));

        String jsonStreamConfig = null;
        long offset = 0L;

        while (offset < endOffset) {
            for (ConsumerRecord<String, String> record : restoreConsumer.poll(100).records(storePartition)) {
                if (record.key().equals(appId)) jsonStreamConfig = record.value();
            }
            offset = restoreConsumer.position(storePartition);
            log.info("Recover from kafka offset[{}], endOffset[{}]", offset, endOffset);
        }

        if (jsonStreamConfig != null) {
            log.info("Find stream configuration with app id [{}]", appId);
            builder.updateStreamConfig(jsonStreamConfig);
        } else {
            log.info("Don't find any stream configuration with app id [{}]", appId);
        }
    }

    @Override
    public void run() {
        currentThread().setName("KafkaBootstraper");

        while (!closed.get()) {
            log.debug("Searching stream configuration with app id [{}]", appId);
            try {
                for (ConsumerRecord<String, String> record : restoreConsumer.poll(15000).records(storePartition)) {
                    if (record.key().equals(appId)) {
                        log.info("Find stream configuration with app id [{}]", appId);
                        builder.updateStreamConfig(record.value());
                    }
                }
            } catch (WakeupException e) {
                if (!closed.get()) throw e;
                log.info("Closing restore consumer ...");
                restoreConsumer.close();
            } catch (PlanBuilderException e) {
                log.error("The stream config isn't valid, try again!", e);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void close() {
        closed.set(true);
        restoreConsumer.wakeup();
        log.info("Stop KafkaBootstraper service");
    }
}
