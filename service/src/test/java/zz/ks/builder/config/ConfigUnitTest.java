package zz.ks.builder.config;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConfigUnitTest {

    @Test
    public void getConfigurationFromFile() throws IOException {
        Config myConfiguration = new Config(Thread.currentThread().getContextClassLoader().getResource("config-test.json").getFile());

        Map<String, Object> expectedConfig = new HashMap<>();

        expectedConfig.put("property-1", "value-1");
        expectedConfig.put("property-2", 100);
        expectedConfig.put("property-3", 50.0);
        expectedConfig.put("property-4", true);

        assertEquals(expectedConfig, myConfiguration.config);
    }

    @Test
    public void getConfigurationFromMap() throws IOException {
        Map<String, Object> myConfigMap = new HashMap<>();
        myConfigMap.put("property-1", "value-1");
        myConfigMap.put("property-2", 100);
        myConfigMap.put("property-3", 50.0);
        myConfigMap.put("property-4", true);

        Config myConfiguration = new Config(Thread.currentThread().getContextClassLoader().getResource("config-test.json").getFile());

        Map<String, Object> expectedConfig = new HashMap<>();
        expectedConfig.put("property-1", "value-1");
        expectedConfig.put("property-2", 100);
        expectedConfig.put("property-3", 50.0);
        expectedConfig.put("property-4", true);

        assertEquals(expectedConfig, myConfiguration.config);
    }

    @Test
    public void getValueOrDefaultValue() {
        Config myConfiguration = new Config();

        myConfiguration
                .put("property-1", "value-1");

        Map<String, Object> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put("property-1", "value-1");

        assertEquals(expectedConfiguration, myConfiguration.config);

        assertEquals("value-1", myConfiguration.<String>get("property-1"));
        assertNull(myConfiguration.<Integer>get("property-2"));
        assertEquals(new Integer(100), myConfiguration.<Integer>getOrDefault("property-2", 100));
        assertNull(myConfiguration.<Integer>get("property-3"));
        assertEquals(new Boolean(true), myConfiguration.<Boolean>getOrDefault("property-3", true));
    }

    @Test
    public void putValuesAndNotNull() {
        Config myConfiguration = new Config();

        myConfiguration
                .put("property-1", "value-1")
                .put("property-2", 100)
                .put("property-3", 50.0)
                .put("property-4", true);

        Map<String, Object> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put("property-1", "value-1");
        expectedConfiguration.put("property-2", 100);
        expectedConfiguration.put("property-3", 50.0);
        expectedConfiguration.put("property-4", true);

        assertEquals(expectedConfiguration, myConfiguration.config);

        assertEquals("value-1", myConfiguration.<String>get("property-1"));
        assertEquals(new Integer(100), myConfiguration.<Integer>get("property-2"));
        assertEquals(new Double(50.0), myConfiguration.<Double>get("property-3"));
        assertEquals(new Boolean(true), myConfiguration.<Boolean>get("property-4"));
    }

}
