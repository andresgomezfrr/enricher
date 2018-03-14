package io.wizzie.ks.builder.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.serializers.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.VALUE_SERDE_CLASS_CONFIG;

public class Config {
    Map<String, Object> config;
    Properties properties = new Properties();

    public Config() {
        config = new HashMap<>();
    }

    public Config(String configPath) throws IOException {
        init(configPath);
    }

    public Config(Map<String, Object> properties) {
        init(properties);
    }

    private void init(String configPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        init(objectMapper.readValue(new File(configPath), Map.class));
    }

    private void init(Map<String, Object> mapProperties) {
        config = mapProperties;
        properties.putAll(mapProperties);
        properties.put(KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
    }

    public <T> T get(String property) {
        T ret = null;

        if (config != null) {
            ret = (T) config.get(property);
        }

        return ret;
    }

    public <T> T getOrDefault(String property, T defaultValue) {
        T ret = null;

        if (config != null && config.get(property) != null)
            ret = (T) config.get(property);
        else
            ret = defaultValue;

        return ret;
    }

    public Config put(String property, Object value) {
        config.put(property, value);
        properties.put(property, value);
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public static class ConfigProperties {
        public static final String BOOTSTRAPER_CLASSNAME = "bootstraper.classname";
        public static final String METRIC_ENABLE = "metric.enable";
        public static final String METRIC_LISTENERS = "metric.listeners";
        public static final String METRIC_INTERVAL = "metric.interval";
        public static final String MULTI_ID = "multi.id";
    }

    public Config clone() {
        return new Config(new HashMap<>(config));
    }
}
