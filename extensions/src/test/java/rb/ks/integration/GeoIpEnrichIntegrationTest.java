package rb.ks.integration;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;
import rb.ks.serializers.JsonDeserializer;
import rb.ks.serializers.JsonSerde;
import rb.ks.serializers.JsonSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;
import static rb.ks.builder.config.Config.ConfigProperties.BOOTSTRAPER_CLASSNAME;

public class GeoIpEnrichIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_STREAM_TOPIC = "input";

    private static final String BOOTSTRAP_TOPIC = "__enricher_bootstrap";

    private static final String OUTPUT_TOPIC = "output";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_STREAM_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(BOOTSTRAP_TOPIC, 1, REPLICATION_FACTOR);


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
    public void geoIpEnrichShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Config configuration = new Config(streamsConfiguration);
        configuration.put(BOOTSTRAPER_CLASSNAME, "rb.ks.builder.bootstrap.KafkaBootstraper");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append("{")
                .append("\"joiners\":[ ],\n")
                .append("\"enrichers\":[\n" +
                        "    {\n" +
                        "      \"name\": \"geoipEnrich\",\n" +
                        "      \"className\": \"rb.ks.enrichment.geoip.GeoIpEnrich\",\n" +
                        "      \"properties\": {\n" +
                        "        \"asn.db.path\": \"" + classLoader.getResource("asn.dat").getPath() + "\",\n" +
                        "        \"asn6.db.path\": \"" + classLoader.getResource("asnv6.dat").getPath() + "\",\n" +
                        "        \"city.db.path\": \"" + classLoader.getResource("city.dat").getPath() + "\",\n" +
                        "        \"city6.db.path\": \"" + classLoader.getResource("cityv6.dat").getPath() + "\",\n" +
                        "        \"src.country.code.dim\": \"src_country_code\",\n" +
                        "        \"dst.country.code.dim\": \"dst_country_code\",\n" +
                        "        \"src.dim\": \"src\",\n" +
                        "        \"dst.dim\": \"dst\",\n" +
                        "        \"src.as.name.dim\": \"src_as_name\",\n" +
                        "        \"dst.as.name.dim\": \"dst_as_name\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  ],\n")
                .append("  \"queries\": {\n" +
                        "    \"query1\": \"SELECT * FROM STREAM input ENRICH WITH geoipEnrich INSERT INTO TABLE output\"\n" +
                        "  }")
                .append("}")
                ;

        String jsonConfig = stringBuilder.toString();

        KeyValue<String, String> jsonConfigKv = new KeyValue<>(appId, jsonConfig);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(BOOTSTRAP_TOPIC, Collections.singletonList(jsonConfigKv), producerConfig);

        Builder builder = new Builder(configuration);

        Map<String, Object> message1 = new HashMap<>();
        message1.put("src", "8.8.8.8");
        message1.put("dst", "8.8.4.4");

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY_A", message1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_STREAM_TOPIC, Arrays.asList(kvStream1), producerConfig);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("src", "8.8.8.8");
        expectedData.put("dst", "8.8.4.4");
        expectedData.put("dst_country_code", "US");
        expectedData.put("src_country_code", "US");
        expectedData.put("dst_as_name", "Google Inc.");
        expectedData.put("src_as_name", "Google Inc.");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>("KEY_A", expectedData);

        List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput);

        builder.close();
    }

    @AfterClass
    public static void stopKafkaCluster() {
        CLUSTER.stop();
    }

    public static String getFileContent(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        StringBuilder stringBuffer = new StringBuilder();

        String line;

        while ((line = bufferedReader.readLine()) != null) {

            stringBuffer.append(line).append("\n");
        }

        return stringBuffer.toString();
    }

}
