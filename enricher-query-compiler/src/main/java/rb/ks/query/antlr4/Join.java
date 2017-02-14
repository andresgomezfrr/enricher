package rb.ks.query.antlr4;

import java.util.ArrayList;
import java.util.List;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Join {

    List<String> dimensions;
    Stream topic;
    String joinerClass;

    public Join(Stream topic, String joinerClass) {
        this(topic, joinerClass, new ArrayList<>());
    }

    public Join(Stream topic, String joinerClass, List<String> dimensions) {
        this.topic = checkNotNull(topic, "\"Topic\" attribute is required");
        this.joinerClass = checkNotNull(joinerClass, "\"joinerClass\" attribute is required");
        this.dimensions = checkNotNull(dimensions, "\"dimensions\" is required");
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

    public void setJoinerClass(String newClassName) {
        joinerClass = newClassName;
    }

    public String getJoinerClass() {
        return joinerClass;
    }

    public void validate() {
        checkNotNull(dimensions, "At least a dimensions is required");
        checkNotNull(topic, "At least a topic is required");
        checkNotNull(joinerClass, "At least a joiner class is required");

        topic.validate();
    }

}
