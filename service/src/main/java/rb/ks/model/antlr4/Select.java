package rb.ks.model.antlr4;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Select {

    List<String> dimensionsList = new ArrayList<>();
    Stream topic;

    public void setDimensionsList(List<String> newDimensionsList) {
        dimensionsList = newDimensionsList;
    }

    public void addNewDimension(String dimension) {
        dimensionsList.add(dimension);
    }

    public void setTopic(Stream newStream) {
        topic = newStream;
    }

    public void validate() {
        checkNotNull(dimensionsList, "Dimension is required");
        checkNotNull(topic, "Input is required");
        topic.validate();
    }

}
