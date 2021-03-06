package io.wizzie.ks.enricher.builder;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.ks.enricher.base.builder.config.ConfigProperties;
import io.wizzie.ks.enricher.enrichment.join.BaseJoiner;
import io.wizzie.ks.enricher.enrichment.join.Joiner;
import io.wizzie.ks.enricher.enrichment.join.QueryableBackJoiner;
import io.wizzie.ks.enricher.enrichment.join.QueryableJoiner;
import io.wizzie.ks.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.ks.enricher.enrichment.simple.Enrich;
import io.wizzie.ks.enricher.exceptions.EnricherNotFound;
import io.wizzie.ks.enricher.exceptions.JoinerNotFound;
import io.wizzie.ks.enricher.model.PlanModel;
import io.wizzie.ks.enricher.model.exceptions.PlanBuilderException;
import io.wizzie.ks.enricher.query.antlr4.Join;
import io.wizzie.ks.enricher.query.antlr4.Select;
import io.wizzie.ks.enricher.query.antlr4.Stream;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.wizzie.ks.enricher.base.utils.Constants.__KEY;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;
    Map<String, KStream<String, Map<String, Object>>> streams;
    Map<String, KTable<String, Map<String, Object>>> tables;
    Map<String, Joiner> joiners = new HashMap<>();
    Map<String, Enrich> enrichers = new HashMap<>();
    List<String> globalTopics = new LinkedList<>();

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;
        this.streams = new HashMap<>();
        this.tables = new HashMap<>();
        this.globalTopics = config.getOrDefault(ConfigProperties.GLOBAL_TOPICS, new LinkedList<String>());
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    public StreamsBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate(config.clone());
        clean();

        StreamsBuilder builder = new StreamsBuilder();

        buildInstances(model);
        addStreams(model, builder);
        addTables(model, builder);
        addEnriches(model);
        addInserts(model);

        return builder;
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

    private void addStreams(PlanModel model, StreamsBuilder builder) {

        Random random = new Random();

        model.getQueries().entrySet().forEach(entry -> {
            Select selectQuery = entry.getValue().getSelect();

            List<String> topicStreams = selectQuery.getStreams().stream()
                    .filter(s -> !s.isTable())
                    .map(Stream::getName)
                    .collect(Collectors.toList());

            if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                topicStreams = topicStreams.stream()
                        .map(topic -> globalTopics.contains(topic) ?
                                topic : String.format("%s_%s", appId, topic))
                        .collect(Collectors.toList());
            }

            KStream<String, Map<String, Object>> stream =
                    builder.stream(topicStreams);

            if(config.getOrDefault(ConfigProperties.BYPASS_NULL_KEYS, false)){
                stream = stream.map((s, stringObjectMap) -> {
                    if(s == null) {
                        return new KeyValue<>(String.format("%s-%s", "NULL-KEY", random.nextInt(1000)), stringObjectMap);
                    }else
                        return new KeyValue<>(s, stringObjectMap);
                });
            }

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

    private void addTables(PlanModel model, StreamsBuilder builder) {
        model.getQueries().entrySet().forEach(entry -> {
            List<Join> joins = entry.getValue().getJoins();

            joins.forEach(join -> {

                if (!join.getStream().isTable()) {
                    log.warn("Join beetween stream isn't supported yet! The join is changed to use stream-table join");
                }

                String tableName;

                if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                    tableName = globalTopics.contains(join.getStream().getName()) ?
                            join.getStream().getName() : String.format("%s_%s", appId, join.getStream().getName());
                } else {
                    tableName = join.getStream().getName();
                }

                KTable<String, Map<String, Object>> table;

                if (tables.containsKey(tableName)) {
                    table = tables.get(tableName);
                } else {
                    table = builder.table(tableName);
                    tables.put(tableName, table);
                }

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
                    stream = stream.selectKey((key, value) -> {
                        String newKey;
                        if (value.containsKey(join.getPartitionKey())) {
                            newKey = value.get(join.getPartitionKey()).toString();
                        } else {
                            newKey = key;
                        }

                        return newKey;
                    });
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

            if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                outputStream = String.format("%s_%s", appId, outputStream);
            }

            stream.to(outputStream);
        });
    }

    private void clean() {
        streams.clear();
        tables.clear();
        joiners.clear();
        enrichers.clear();
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
