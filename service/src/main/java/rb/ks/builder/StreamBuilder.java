package rb.ks.builder;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.config.Config;
import rb.ks.enrichment.join.Joiner;
import rb.ks.enrichment.join.QueryableJoiner;
import rb.ks.enrichment.simple.BaseEnrich;
import rb.ks.enrichment.simple.Enrich;
import rb.ks.exceptions.EnricherNotFound;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.enrichment.join.BaseJoiner;
import rb.ks.metrics.MetricsManager;
import rb.ks.model.PlanModel;
import rb.ks.query.antlr4.Join;
import rb.ks.query.antlr4.Select;
import rb.ks.query.antlr4.Stream;
import rb.ks.exceptions.JoinerNotFound;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;
    Map<String, KStream<String, Map<String, Object>>> streams;
    Map<String, Joiner> joiners = new HashMap<>();
    Map<String, Enrich> enrichers = new HashMap<>();

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;
        this.streams = new HashMap<>();
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    public KStreamBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate();
        clean();

        KStreamBuilder builder = new KStreamBuilder();

        buildInstances(model);
        addStresms(model, builder);
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

    private void addStresms(PlanModel model, KStreamBuilder builder) {
        model.getQueries().entrySet().forEach(entry -> {
            Select selectQuery = entry.getValue().getSelect();

            List<String> topicStreams = selectQuery.getStreams().stream()
                    .filter(s -> !s.isTable())
                    .map(Stream::getName)
                    .collect(Collectors.toList());

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
                String tableName = join.getStream().getName();
                KTable<String, Map<String, Object>> table = builder.table(tableName);

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

                Joiner joiner = joiners.get(join.getJoinerName());
                if (joiner == null) throw new JoinerNotFound("BaseJoiner " + join.getJoinerName() + " not found!");

                if (joiner instanceof BaseJoiner) {
                    stream = stream.leftJoin(table, (BaseJoiner) joiner);
                } else if (joiner instanceof QueryableJoiner) {
                    KStream<String, Map<String, Object>> joinStream = stream.leftJoin(table, (QueryableJoiner) joiner);

                    joinStream
                            .branch((key, value) -> value.containsKey("type") && value.get("type").equals("joiner-query"))[0]
                            .mapValues(value -> {
                                Map<String, Object> newValue = new HashMap<>(value);
                                newValue.remove("message");
                                newValue.put("table", tableName);
                                return newValue;
                            })
                            .to("__enricher_queryable");

                    stream = joinStream.mapValues(message -> (Map<String, Object>) message.get("message"));
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
            stream.to(insert.getName());
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
