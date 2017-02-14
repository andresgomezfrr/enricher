package rb.ks.enrichment.geoip;

import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

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
        properties.put("asn.db.path", asn.getAbsolutePath());
        properties.put("asn6.db.path", asnv6.getAbsolutePath());
        properties.put("city.db.path", city.getAbsolutePath());
        properties.put("city6.db.path", cityv6.getAbsolutePath());
        properties.put("src.country.code.dim", "src_country_code");
        properties.put("dst.country.code.dim", "dst_country_code");
        properties.put("src.dim", "src");
        properties.put("dst.dim", "dst");
        properties.put("src.as.name.dim", "src_as_name");
        properties.put("dst.as.name.dim", "dst_as_name");
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

        Map<String, Object> result = geoIpEnrich.enrich(message);
        assertEquals(expected, result);

        geoIpEnrich.stop();
    }
}
