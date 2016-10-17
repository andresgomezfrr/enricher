package rb.ks.query.antlr4;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class EnrichWith {
    String functionName;
    String configKey;

    public EnrichWith(String functionName, String configKey) {
        this.functionName = checkNotNull(functionName, "<functionName> attribute is required");
        this.configKey = checkNotNull(configKey, "<configKey> attribute is required");
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String newFunctionName) {
        functionName = newFunctionName;
    }

    public String getConfigKey() {
        return configKey;
    }

    public void setConfigKey(String newConfigKey) {
        configKey = newConfigKey;
    }
}
