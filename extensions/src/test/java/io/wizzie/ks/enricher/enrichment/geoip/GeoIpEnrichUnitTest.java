package io.wizzie.ks.enricher.enrichment.geoip;


import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static io.wizzie.ks.enricher.enrichment.utils.Constants.*;
import static org.junit.Assert.assertEquals;

public class GeoIpEnrichUnitTest {

    @Test
    public void enrichWithGeoIp() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File asn = new File(classLoader.getResource("asn.dat").getFile());
        File asnv6 = new File(classLoader.getResource("asnv6.dat").getFile());
        File city = new File(classLoader.getResource("city.dat").getFile());
        File cityv6 = new File(classLoader.getResource("cityv6.dat").getFile());

        Map<String, Object> properties = new HashMap<>();
        properties.put(ASN_DB_PATH, asn.getAbsolutePath());
        properties.put(ASN6_DB_PATH, asnv6.getAbsolutePath());
        properties.put(CITY_DB_PATH, city.getAbsolutePath());
        properties.put(CITY6_DB_PATH, cityv6.getAbsolutePath());
        properties.put(SRC_COUNTRY_CODE_DIM, "src_country_code");
        properties.put(DST_COUNTRY_CODE_DIM, "dst_country_code");
        properties.put(SRC_DIM, "src");
        properties.put(DST_DIM, "dst");
        properties.put(SRC_AS_NAME_DIM, "src_as_name");
        properties.put(DST_AS_NAME_DIM, "dst_as_name");
        geoIpEnrich.init(properties, null);

        Map<String, Object> message = new HashMap<>();
        message.put("src", "8.8.8.8");
        message.put("dst", "8.8.4.4");

        Map<String, Object> expected = new HashMap<>();
        expected.put("src", "8.8.8.8");
        expected.put("dst", "8.8.4.4");
        expected.put("dst_country_code", "US");
        expected.put("src_country_code", "US");
        expected.put("dst_as_name", "Google Inc.");
        expected.put("src_as_name", "Google Inc.");
        expected.put("src_city", "Mountain View");

        Map<String, Object> result = geoIpEnrich.enrich(message);
        assertEquals(expected, result);

        geoIpEnrich.stop();
    }

    @Test
    public void defaultPropertiesShouldWorkCorrectly() {
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        Map<String, Object> properties = new HashMap<>();

        geoIpEnrich.init(properties, null);

        assertEquals("dst", geoIpEnrich.DST_IP);
        assertEquals("src", geoIpEnrich.SRC_IP);
        assertEquals("dst_country_code", geoIpEnrich.DST_COUNTRY_CODE);
        assertEquals("src_country_code", geoIpEnrich.SRC_COUNTRY_CODE);
        assertEquals("dst_as_name", geoIpEnrich.DST_AS_NAME);
        assertEquals("src_as_name", geoIpEnrich.SRC_AS_NAME);
    }

    @Test
    public void dimensionNameShouldBeCorrectly(){
        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        Map<String, Object> properties = new HashMap<>();
        properties.put(SRC_COUNTRY_CODE_DIM, "my_src_country_code_dim");
        properties.put(DST_COUNTRY_CODE_DIM, "my_dst_country_code_dim");
        properties.put(SRC_DIM, "my_src_dim");
        properties.put(DST_DIM, "my_dst_dim");
        properties.put(SRC_AS_NAME_DIM, "my_src_as_name_dim");
        properties.put(DST_AS_NAME_DIM, "my_dst_as_name_dim");

        geoIpEnrich.init(properties, null);

        assertEquals("my_dst_dim", geoIpEnrich.DST_IP);
        assertEquals("my_src_dim", geoIpEnrich.SRC_IP);
        assertEquals("my_dst_country_code_dim", geoIpEnrich.DST_COUNTRY_CODE);
        assertEquals("my_src_country_code_dim", geoIpEnrich.SRC_COUNTRY_CODE);
        assertEquals("my_dst_as_name_dim", geoIpEnrich.DST_AS_NAME);
        assertEquals("my_src_as_name_dim", geoIpEnrich.SRC_AS_NAME);
    }
}
