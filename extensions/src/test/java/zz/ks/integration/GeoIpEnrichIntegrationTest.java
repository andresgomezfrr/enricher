package zz.ks.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
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

public class GeoIpEnrichIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_STREAM_TOPIC = "input";

    private static final String BOOTSTRAP_TOPIC = "__enricher_bootstrap";

    private static final String OUTPUT_TOPIC = "output";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    private static ObjectMapper mapper;

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

        mapper = new ObjectMapper();
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
        configuration.put(Config.ConfigProperties.BOOTSTRAPER_CLASSNAME, "zz.ks.builder.bootstrap.KafkaBootstraper");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Map<String, Object> geoIpProperties = new HashMap<>();
        geoIpProperties.put("asn.db.path", classLoader.getResource("asn.dat").getPath());
        geoIpProperties.put("asn6.db.path", classLoader.getResource("asnv6.dat").getPath());
        geoIpProperties.put("city.db.path", classLoader.getResource("city.dat").getPath());
        geoIpProperties.put("city6.db.path", classLoader.getResource("cityv6.dat").getPath());
        geoIpProperties.put("src.country.code.dim", "src_country_code");
        geoIpProperties.put("dst.country.code.dim", "dst_country_code");
        geoIpProperties.put("src.dim", "src");
        geoIpProperties.put("dst.dim", "dst");
        geoIpProperties.put("src.as.name.dim", "src_as_name");
        geoIpProperties.put("dst.as.name.dim", "dst_as_name");

        Map<String, Object> geoIpEnricher = new HashMap<>();
        geoIpEnricher.put("name", "geoipEnrich");
        geoIpEnricher.put("className", "zz.ks.enrichment.geoip.GeoIpEnrich");
        geoIpEnricher.put("properties", geoIpProperties);

        Map<String, Object> queries = new HashMap<>();
        queries.put("query1", "SELECT * FROM STREAM input ENRICH WITH geoipEnrich INSERT INTO TABLE output");

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("enrichers", Collections.singletonList(geoIpEnricher));
        jsonMap.put("queries", queries);

        KeyValue<String, String> jsonConfigKv = new KeyValue<>(appId, mapper.writeValueAsString(jsonMap));

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
