package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import rb.ks.exceptions.PlanBuilderException;
import rb.ks.query.EnricherCompiler;
import rb.ks.query.antlr4.Join;
import rb.ks.query.antlr4.Query;

import java.util.*;
import java.util.stream.Collectors;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class PlanModel {
    Map<String, Query> queries = new LinkedHashMap<>();
    List<JoinerModel> joiners = new ArrayList<>();
    List<EnricherModel> enrichers = new ArrayList<>();

    @JsonCreator
    public PlanModel(@JsonProperty("joiners") List<JoinerModel> joiners,
                     @JsonProperty("queries") Map<String, String> queries,
                     @JsonProperty("enrichers") List<EnricherModel> enrichers) {
        checkNotNull(joiners, "joiners cannot be null");
        checkNotNull(queries, "queries cannot be null");

        this.joiners.addAll(joiners);

        queries.forEach((name, queryString) -> {
            Query query = EnricherCompiler.parse(queryString);
            this.queries.put(name, query);

        });

        if(enrichers != null) this.enrichers.addAll(enrichers);
    }

    @JsonProperty
    public Map<String, Query> getQueries() {
        return queries;
    }

    @JsonProperty
    public List<JoinerModel> getJoiners() {
        return joiners;
    }

    @JsonProperty
    public List<EnricherModel> getEnrichers() {
        return enrichers;
    }

    public void validate() throws PlanBuilderException {

        List<String> definedJoiners = this.joiners.stream().map(joiner -> joiner.className).collect(Collectors.toList());
        List<String> definedEnrichers = this.enrichers.stream().map(enricher -> enricher.className).collect(Collectors.toList());

        for(Map.Entry<String, Query> queryEntry : queries.entrySet()) {
            List<String> enrichers = queryEntry.getValue().getEnrichWiths();
            List<Join> joiners = queryEntry.getValue().getJoins();

            if(enrichers != null) {
                for(String enricher: enrichers) {
                    if(!definedEnrichers.contains(enricher)) {
                        throw new PlanBuilderException(String.format("Enricher[%s]: Not defined", enricher));
                    }
                }
            }

            if(joiners != null) {
                for(Join joiner : joiners) {
                    String joinerName = joiner.getJoinerName();
                    if(!definedJoiners.contains(joinerName)) {
                        throw new PlanBuilderException(String.format("Joiner[%s]: Not defined", joinerName));
                    }
                }
            }

        }
    }

    public String printExecutionPlan() {
        return "";
    }
}
