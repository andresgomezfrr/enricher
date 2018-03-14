package rb.ks.query.antlr4;

import java.util.Collections;
import java.util.List;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Join {

    List<String> dimensions;
    Stream topic;
    String joinerName;
    List<EnrichWith> enrichWiths;

    public Join(Stream topic, String joinerName) {
        this(topic, joinerName, Collections.EMPTY_LIST);
    }

    public Join(Stream topic, String joinerName, List<String> dimensions) {
        this(topic, joinerName, dimensions, Collections.EMPTY_LIST);
    }

    public Join(Stream topic, String joinerName, List<String> dimensions, List<EnrichWith> enrichWiths) {
        this.topic = checkNotNull(topic, "<topic> attribute is required");
        this.joinerName = checkNotNull(joinerName, "<joinerName> attribute is required");
        this.dimensions = checkNotNull(dimensions, "<dimensions> is required");
        this.enrichWiths = checkNotNull(enrichWiths, "<enrichWith> is required");
    }

    public void setStream(Stream stream) {
        topic = stream;
    }

    public Stream getStream() {
        return topic;
    }

    public void setDimensions(List<String> newDimensions) {
        dimensions = newDimensions;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setJoinerName(String newClassName) {
        joinerName = newClassName;
    }

    public String getJoinerName() {
        return joinerName;
    }

    public void setEnrichWiths(List<EnrichWith> enrichWiths) {
        this.enrichWiths = enrichWiths;
    }

    public List<EnrichWith> getEnrichWiths() {
        return enrichWiths;
    }

    public void validate() {
        checkNotNull(dimensions, "At least a dimensions list is required");
        checkNotNull(topic, "At least a topic is required");
        checkNotNull(joinerName, "At least a joiner class name is required");
        checkNotNull(enrichWiths, "At least a enrich with list is required");

        topic.validate();
    }

}
