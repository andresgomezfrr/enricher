package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class JoinerModel {
    String name;
    String className;

    @JsonCreator
    public JoinerModel(@JsonProperty("name") String name,
                       @JsonProperty("className") String className) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(className, "className cannot be null");

        this.name = name;
        this.className = className;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getClassName() {
        return className;
    }
}
