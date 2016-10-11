package rb.ks.model.antlr4;

import java.util.List;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Join {

    List<String> dimensionsList;

    Stream topic;

    String className;

    public void setStream(Stream stream) {
        topic = stream;
    }

    public void setDimensions(List<String> newDimensionsList) {
        dimensionsList = newDimensionsList;
    }

    public void setClassName(String newClassName) {
        className = newClassName;
    }

    public void validate() {
        checkNotNull(dimensionsList, "Dimensions is required");
        checkNotNull(topic, "Stream is required");
        checkNotNull(className, "Class is required");

        topic.validate();
    }

}
