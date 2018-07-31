package io.wizzie.enricher.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EnricherModelUnitTest {

    ObjectMapper mapper;

    @Before
    public void initTest() {
        mapper = new ObjectMapper();
    }

    @Test
    public void enricherModelIsBuiltCorrectly() throws IOException {

        String json = "{\"name\": \"myEnricherName1\", \"className\": \"pkg1.pkg2.Class1\", \"properties\":{\"propA\":\"valueA\", \"propB\":\"valueB\"}}uff";

        EnricherModel enricherModel = mapper.readValue(json, EnricherModel.class);

        assertNotNull(enricherModel);

        assertEquals("myEnricherName1", enricherModel.getName());
        assertEquals("pkg1.pkg2.Class1", enricherModel.getClassName());

        Map<String, Object> expectedProperties = new HashMap<>();
        expectedProperties.put("propA", "valueA");
        expectedProperties.put("propB", "valueB");

        assertEquals(expectedProperties, enricherModel.getProperties());

    }


}
