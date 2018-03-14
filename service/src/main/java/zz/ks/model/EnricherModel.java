package zz.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class EnricherModel {
    String name;
    String className;
    Map<String, Object> properties = new HashMap<>();

    @JsonCreator
    public EnricherModel(@JsonProperty("name") String name,
                         @JsonProperty("className") String className,
                         @JsonProperty("properties") Map<String, Object> properties) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(className, "className cannot be null");

        this.name = name;
        this.className = className;

        if(properties != null) this.properties.putAll(properties);
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getClassName() {
        return className;
    }

    @JsonProperty
    public Map<String, Object> getProperties() {
        return properties;
    }
}
