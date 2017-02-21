package io.wizzie.ks.enricher.builder;

import io.wizzie.ks.enricher.builder.config.Config;
import io.wizzie.ks.enricher.enrichment.join.BaseJoiner;
import io.wizzie.ks.enricher.enrichment.join.Joiner;
import io.wizzie.ks.enricher.enrichment.join.QueryableBackJoiner;
import io.wizzie.ks.enricher.enrichment.join.QueryableJoiner;
import io.wizzie.ks.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.ks.enricher.enrichment.simple.Enrich;
import io.wizzie.ks.enricher.exceptions.EnricherNotFound;
import io.wizzie.ks.enricher.exceptions.JoinerNotFound;
import io.wizzie.ks.enricher.exceptions.PlanBuilderException;
import io.wizzie.ks.enricher.metrics.MetricsManager;
import io.wizzie.ks.enricher.model.PlanModel;
import io.wizzie.ks.enricher.query.antlr4.Join;
import io.wizzie.ks.enricher.query.antlr4.Select;
import io.wizzie.ks.enricher.query.antlr4.Stream;
import kafka.admin.TopicCommand;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.wizzie.ks.enricher.utils.Constants.__KEY;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;
    Map<String, KStream<String, Map<String, Object>>> streams;
    Map<String, Joiner> joiners = new HashMap<>();
    Map<String, Enrich> enrichers = new HashMap<>();

    ZkUtils zkUtils;
    ZkClient zkClient;

    final String ZK_CONNECT;
    final String PARTITIONS = "4";
    final String REPLICATION_FACTOR = "1";

    final int ZK_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        ZK_CONNECT = config.get("zookeeper.connect");

        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;
        this.streams = new HashMap<>();

        if(ZK_CONNECT != null) {
            zkClient = createZkClient();
            zkUtils = new ZkUtils(zkClient, new ZkConnection(ZK_CONNECT),false);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate(config.clone());
        clean();

        KStreamBuilder builder = new KStreamBuilder();

        buildInstances(model);
        addStresms(model, builder);
        addTables(model, builder);
        addEnriches(model);
        addInserts(model);

        return builder;
    }

    private ZkClient createZkClient(){
        return new ZkClient(ZK_CONNECT, ZK_TIMEOUT, ZK_TIMEOUT, ZKStringSerializer$.MODULE$);
    }

    private void createTopicIfNotExists(String topic) {
        if(zkClient != null && zkUtils != null) {
            String[] topicArgs = {
                    "--zookeeper", ZK_CONNECT,
                    "--partitions", PARTITIONS,
                    "--replication-factor", REPLICATION_FACTOR,
                    "--create",
                    "--topic", topic,
                    "--if-not-exists"
            };

            TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(topicArgs);
            options.checkArgs();

            TopicCommand.createTopic(zkUtils, options);
        }
    }

    private void buildInstances(PlanModel model) {
        model.getEnrichers().forEach(enrichModel -> {
            try {
                Enrich enrich = makeInstance(enrichModel.getClassName());
                enrich.init(enrichModel.getProperties(), metricsManager);
                enrichers.put(enrichModel.getName(), enrich);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the function {}", enrichModel.getClassName());
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the function " + enrichModel.getClassName(), e);
            }
        });

        model.getJoiners().forEach(joinerModel -> {
            try {
                Joiner joiner = makeInstance(joinerModel.getClassName());
                joiner.init(joinerModel.getName());
                joiners.put(joinerModel.getName(), joiner);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the function {}", joinerModel.getClassName());
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the function " + joinerModel.getClassName(), e);
            }
        });
    }

    private void addStresms(PlanModel model, KStreamBuilder builder) {
        model.getQueries().entrySet().forEach(entry -> {
            Select selectQuery = entry.getValue().getSelect();

            List<String> topicStreams = selectQuery.getStreams().stream()
                    .filter(s -> !s.isTable())
                    .map(Stream::getName)
                    .collect(Collectors.toList());

            if (config.getOrDefault(Config.ConfigProperties.MULTI_ID, false)) {
                topicStreams = topicStreams.stream()
                        .map(topic -> String.format("%s_%s", appId, topic))
                        .collect(Collectors.toList());
            }

            topicStreams.forEach(topic -> createTopicIfNotExists(topic));

            KStream<String, Map<String, Object>> stream =
                    builder.stream(topicStreams.toArray(new String[topicStreams.size()]));

            List<String> dimensions = selectQuery.getDimensions();
            if (!dimensions.contains("*")) {
                stream = stream.mapValues(value -> {
                    Map<String, Object> filterValue = new HashMap<>();

                    dimensions.forEach(dim -> {
                        if (value.containsKey(dim)) {
                            filterValue.put(dim, value.get(dim));
                        }
                    });

                    return filterValue;
                });
            }

            streams.put(entry.getKey(), stream);
        });
    }

    private void addTables(PlanModel model, KStreamBuilder builder) {
        model.getQueries().entrySet().forEach(entry -> {
            List<Join> joins = entry.getValue().getJoins();

            joins.forEach(join -> {

                if (!join.getStream().isTable()) {
                    log.warn("Join beetween stream isn't supported yet! The join is changed to use stream-table join");
                }

                String tableName;

                if (config.getOrDefault(Config.ConfigProperties.MULTI_ID, false)) {
                    tableName = String.format("%s_%s", appId, join.getStream().getName());
                } else {
                    tableName = join.getStream().getName();
                }

                createTopicIfNotExists(tableName);

                KTable<String, Map<String, Object>> table = builder.table(tableName, String.format("__%s_%s", appId, tableName));

                List<String> dimensions = join.getDimensions();
                if (!dimensions.contains("*")) {
                    table = table.mapValues(value -> {
                        Map<String, Object> filterValue = new HashMap<>();

                        dimensions.forEach(dim -> {
                            if (value.containsKey(dim)) {
                                filterValue.put(dim, value.get(dim));
                            }
                        });

                        return filterValue;
                    });
                }

                KStream<String, Map<String, Object>> stream = streams.get(entry.getKey());

                if (!join.getPartitionKey().equals(__KEY)) {
                    stream = stream
                            .map((key, value) -> {
                                String newKey;
                                if (value.containsKey(join.getPartitionKey())) {
                                    newKey = value.get(join.getPartitionKey()).toString();
                                } else {
                                    newKey = key;
                                }

                                return new KeyValue<>(newKey, value);
                            })
                            .through(
                                    (key, value, numPartitions) -> Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions,
                                    String.format("__%s_enricher_%s_partition_by_%s",
                                            appId, entry.getKey(), join.getPartitionKey())
                            );
                }

                Joiner joiner = joiners.get(join.getJoinerName());
                if (joiner == null) throw new JoinerNotFound("BaseJoiner " + join.getJoinerName() + " not found!");

                if (joiner instanceof BaseJoiner) {
                    stream = stream.leftJoin(table, (BaseJoiner) joiner);
                } else if (joiner instanceof QueryableJoiner) {
                    KStream<String, Map<String, Object>> joinStream = stream.leftJoin(table, (QueryableJoiner) joiner);

                    joinStream
                            .branch((key, value) -> !((Boolean) value.get("joiner-status")))[0]
                            .mapValues(value -> {
                                Map<String, Object> newValue = new HashMap<>(value);
                                newValue.remove("message");
                                newValue.put("table", tableName);
                                return newValue;
                            })
                            .to("__enricher_queryable");

                    stream = joinStream.mapValues(message -> (Map<String, Object>) message.get("message"));
                } else if (joiner instanceof QueryableBackJoiner) {
                    KStream<String, Map<String, Object>> joinStream = stream.leftJoin(table, (QueryableBackJoiner) joiner);

                    joinStream
                            .branch((key, value) -> !((Boolean) value.get("joiner-status")))[0]
                            .mapValues(value -> {
                                Map<String, Object> newValue = new HashMap<>(value);
                                newValue.remove("message");
                                newValue.put("table", tableName);
                                return newValue;
                            })
                            .to("__enricher_queryable");

                    List<String> topics = model.getQueries().get(entry.getKey()).getSelect().getStreams().stream()
                            .filter(s -> !s.isTable())
                            .map(Stream::getName)
                            .collect(Collectors.toList());

                    //Workaround: Select the first topic to send back the data.
                    joinStream
                            .branch((key, value) -> !((Boolean) value.get("joiner-status")))[0]
                            .mapValues(message -> (Map<String, Object>) message.get("message"))
                            .to(topics.get(0));

                    stream = joinStream
                            .filter((key, value) -> (Boolean) value.get("joiner-status"))
                            .mapValues(message -> (Map<String, Object>) message.get("message"));

                }
                streams.put(entry.getKey(), stream);
            });
        });
    }

    private void addEnriches(PlanModel model) {
        model.getQueries().entrySet().forEach(entry -> {
            List<String> enrichWiths = entry.getValue().getEnrichWiths();

            enrichWiths.forEach(enrichName -> {
                KStream<String, Map<String, Object>> stream = streams.get(entry.getKey());

                Enrich enrich = enrichers.get(enrichName);
                if (enrich == null) throw new EnricherNotFound("Enricher " + enrichName + " not found!");

                if (enrich instanceof BaseEnrich) {
                    stream = stream.mapValues((BaseEnrich) enrich);
                } else {
                    log.error("WTF!! The enrich {} isn't a enrich!", enrichName);
                }

                streams.put(entry.getKey(), stream);
            });
        });
    }

    private void addInserts(PlanModel model) {
        model.getQueries().entrySet().forEach(entry -> {
            Stream insert = entry.getValue().getInsert();
            KStream<String, Map<String, Object>> stream = streams.get(entry.getKey());

            String outputStream = insert.getName();

            if (config.getOrDefault(Config.ConfigProperties.MULTI_ID, false)) {
                outputStream = String.format("%s_%s", appId, outputStream);
            }

            createTopicIfNotExists(outputStream);

            stream.to(outputStream);
        });
    }

    private void clean() {
        streams.clear();
    }

    private <T> T makeInstance(String className)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class funcClass = Class.forName(className);
        return (T) funcClass.newInstance();
    }


    public void close() {
        clean();
    }
}
