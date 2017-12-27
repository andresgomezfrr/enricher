package io.wizzie.ks.enricher.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JoinerModelUnitTest {


    ObjectMapper mapper;

    @Before
    public void initTest() {
        mapper = new ObjectMapper();
    }

    @Test
    public void enricherModelIsBuiltCorrectly() throws IOException {

        String json = "{\"name\": \"myJoinerName1\", \"className\": \"pkg1.pkg2.Class1\"}";

        JoinerModel joinerModel = mapper.readValue(json, JoinerModel.class);

        assertNotNull(joinerModel);

        assertEquals("myJoinerName1", joinerModel.getName());
        assertEquals("pkg1.pkg2.Class1", joinerModel.getClassName());

    }


}
