package zz.ks.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import zz.ks.builder.Builder;
import zz.ks.builder.config.Config;
import zz.ks.serializers.JsonDeserializer;
import zz.ks.serializers.JsonSerde;
import zz.ks.serializers.JsonSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class QueryPartitionByIntegrationTest {


    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_STREAM_1_TOPIC = "stream1";
    private static final String INPUT_STREAM_2_TOPIC = "stream2";
    private static final String INPUT_TABLE_1_TOPIC = "table1";


    private static final String OUTPUT_1_TOPIC = "output1";
    private static final String OUTPUT_2_TOPIC = "output2";

    private static final String appId = UUID.randomUUID().toString();


    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_STREAM_1_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT_STREAM_2_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT_TABLE_1_TOPIC, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(OUTPUT_1_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_2_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(String.format("__%s_enricher_query1_partition_by_b", appId), 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(String.format("__%s_enricher_query2_partition_by_u", appId), 2, REPLICATION_FACTOR);


        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void queryWithDifferentPartitionByShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();


        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Config configuration = new Config(streamsConfiguration);
        configuration.put("file.bootstraper.path", Thread.currentThread().getContextClassLoader().getResource("query-partition-by-integration-test.json").getFile());
        configuration.put(Config.ConfigProperties.BOOTSTRAPER_CLASSNAME, "zz.ks.builder.bootstrap.FileBootstraper");

        Builder builder = new Builder(configuration);

        Map<String, Object> message1 = new HashMap<>();
        message1.put("a", "VALUE_A");
        message1.put("b", "VALUE_B");
        message1.put("c", "VALUE_C");

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY", message1);

        Map<String, Object> message2 = new HashMap<>();
        message2.put("u", "VALUE_U");
        message2.put("v", "VALUE_V");
        message2.put("w", "VALUE_W");

        KeyValue<String, Map<String, Object>> kvStream2 = new KeyValue<>("VALUE_B", message2);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("i", "VALUE_I");
        message3.put("j", "VALUE_J");
        message3.put("k", "VALUE_K");

        KeyValue<String, Map<String, Object>> kvStream3 = new KeyValue<>("VALUE_U", message3);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_STREAM_2_TOPIC, Collections.singletonList(kvStream2), producerConfig);

        List<KeyValue<String, Map>> receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_STREAM_2_TOPIC, 1);
        System.out.println(receivedMessage);

        assertEquals(Collections.singletonList(kvStream2), receivedMessage);




        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TABLE_1_TOPIC, Collections.singletonList(kvStream3), producerConfig);

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_TABLE_1_TOPIC, 1);
        System.out.println(receivedMessage);

        assertEquals(Collections.singletonList(kvStream3), receivedMessage);




        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_STREAM_1_TOPIC, Collections.singletonList(kvStream1), producerConfig);

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_STREAM_1_TOPIC, 1);
        System.out.println(receivedMessage);

        assertEquals(Collections.singletonList(kvStream1), receivedMessage);



        Map<String, Object> expectedData1 = new HashMap<>();
        expectedData1.put("a", "VALUE_A");
        expectedData1.put("b", "VALUE_B");
        expectedData1.put("c", "VALUE_C");
        expectedData1.put("u", "VALUE_U");
        expectedData1.put("v", "VALUE_V");

        KeyValue<String, Map<String, Object>> expectedKvMessage1 = new KeyValue<>("VALUE_B", expectedData1);


        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_1_TOPIC, 1);
        System.out.println(receivedMessage);

        assertEquals(Collections.singletonList(expectedKvMessage1), receivedMessage);


        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("a", "VALUE_A");
        expectedData2.put("b", "VALUE_B");
        expectedData2.put("c", "VALUE_C");
        expectedData2.put("u", "VALUE_U");
        expectedData2.put("v", "VALUE_V");
        expectedData2.put("k", "VALUE_K");

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_2_TOPIC, 1);
        System.out.println(receivedMessage);

        KeyValue<String, Map<String, Object>> expectedKvMessage2 = new KeyValue<>("VALUE_U", expectedData2);

        assertEquals(Collections.singletonList(expectedKvMessage2), receivedMessage);

        builder.close();
    }

    @AfterClass
    public static void stopKafkaCluster() {
        CLUSTER.stop();
    }

}
