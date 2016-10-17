package rb.ks.builder;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.config.Config;
import rb.ks.enrichment.simple.BaseEnrich;
import rb.ks.enrichment.simple.Enrich;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.enrichment.join.Joiner;
import rb.ks.metrics.MetricsManager;
import rb.ks.model.PlanModel;
import rb.ks.query.antlr4.Join;
import rb.ks.query.antlr4.Select;
import rb.ks.query.antlr4.Stream;
import rb.ks.query.exception.JoinerNotFound;
import rb.ks.query.exception.StreamBuilderException;

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
                stream = stream.map((key, value) -> {
                    Map<String, Object> filterValue = new HashMap<>();

                    dimensions.forEach(dim -> {
                        if (value.containsKey(dim)) {
                            filterValue.put(dim, value.get(dim));
                        }
                    });

                    return new KeyValue<>(key, filterValue);
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
                Joiner joiner = joiners.get(join.getJoinerClass());
                if(joiner == null) throw new JoinerNotFound("Joiner " + join.getJoinerClass() + " not found!");
                stream = stream.leftJoin(table, joiner);
                streams.put(entry.getKey(), stream);
            });
        });
    }

    private void addEnriches(PlanModel model, KStreamBuilder builder) {

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
