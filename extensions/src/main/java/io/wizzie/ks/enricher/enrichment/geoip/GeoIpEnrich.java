package io.wizzie.ks.enricher.enrichment.geoip;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import io.wizzie.ks.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.ks.enricher.metrics.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class GeoIpEnrich extends BaseEnrich {
    private static final Logger log = LoggerFactory.getLogger(GeoIpEnrich.class);
    String SRC_COUNTRY_CODE = "src_country_code";
    String DST_COUNTRY_CODE = "dst_country_code";
    String SRC_IP = "src";
    String DST_IP = "dst";
    String SRC_AS_NAME = "src_as_name";
    String DST_AS_NAME = "dst_as_name";

    /**
     * Pattern to to make the comparison with ips v4.
     */
    public Pattern VALID_IPV4_PATTERN = null;
    /**
     * Pattern to to make the comparison with ips v6.
     */
    public Pattern VALID_IPV6_PATTERN = null;
    /**
     * Regular expresion to make the comparison with ipv4 format.
     */
    private static final String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
    /**
     * Regular expresion to make the comparison with ipv6 format.
     */
    private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

    /**
     * Reference on memory cache to city data base.
     */
    LookupService city;
    /**
     * Reference on memory cache to city v6 data base.
     */
    LookupService city6;
    /**
     * Reference on memory cache to asn data base.
     */
    LookupService asn;
    /**
     * Reference on memory cache to asn v6 data base.
     */
    LookupService asn6;

    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        SRC_COUNTRY_CODE = (String) properties.getOrDefault("src.country.code.dim", "src_country_code");
        DST_COUNTRY_CODE = (String) properties.getOrDefault("dst.country.code.dim", "dst_country_code");
        SRC_IP = (String) properties.getOrDefault("src.dim", "src");
        DST_IP = (String) properties.getOrDefault("dst.dim", "dst");
        SRC_AS_NAME = (String) properties.getOrDefault("src.as.name.dim", "src_as_name");
        DST_AS_NAME = (String) properties.getOrDefault("dst.as.name.dim", "dst_as_name");

        String ASN_V6_DB_PATH = (String) properties.getOrDefault("asn6.db.path", "/opt/share/GeoIP/asnv6.dat");
        String ASN_DB_PATH = (String) properties.getOrDefault("asn.db.path", "/opt/share/GeoIP/asn.dat");
        String CITY_V6_DB_PATH = (String) properties.getOrDefault("city6.db.path", "/opt/share/GeoIP/cityv6.dat");
        String CITY_DB_PATH = (String) properties.getOrDefault("city.db.path", "/opt/share/GeoIP/city.dat");

        try {
            city = new LookupService(CITY_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            city6 = new LookupService(CITY_V6_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            asn = new LookupService(ASN_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            asn6 = new LookupService(ASN_V6_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern, Pattern.CASE_INSENSITIVE);
            VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern, Pattern.CASE_INSENSITIVE);
        } catch (IOException ex) {
            log.error(ex.toString(), ex);
        } catch (PatternSyntaxException e) {
            log.error("Unable to compile IP check patterns");
        }
    }

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> geoIPMap = new HashMap<>();
        geoIPMap.putAll(message);

        String src = (String) message.get(SRC_IP);
        String dst = (String) message.get(DST_IP);

        if (src != null) {
            String country_code = null;
            String asn_name = null;

            if (VALID_IPV4_PATTERN != null) {
                country_code = getCountryCode(src);
                asn_name = getAsnName(src);
            }

            if (country_code != null) geoIPMap.put(SRC_COUNTRY_CODE, country_code);
            if (asn_name != null) geoIPMap.put(SRC_AS_NAME, asn_name);
        }

        if (dst != null) {
            String country_code = null;
            String asn_name = null;

            if (VALID_IPV4_PATTERN != null) {
                country_code = getCountryCode(dst);
                asn_name = getAsnName(dst);
            }

            if (country_code != null) geoIPMap.put(DST_COUNTRY_CODE, country_code);
            if (asn_name != null) geoIPMap.put(DST_AS_NAME, asn_name);
        }

        return geoIPMap;
    }

    public void stop() {
        asn.close();
        asn6.close();
        city6.close();
        city.close();
    }

    /**
     * <p>Query if there is a country code for a given IP.</p>
     *
     * @param ip This is the address to query the data base.
     * @return The country code, example: US, ES, FR.
     */
    private String getCountryCode(String ip) {
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        String countryCode = null;
        Location location;

        if (match.matches()) {
            location = city.getLocation(ip);
        } else {
            location = city6.getLocationV6(ip);
        }

        if (location != null) {
            countryCode = location.countryCode;
        }

        return countryCode;
    }

    /**
     * <p>Query if there is a asn for a given IP.</p>
     *
     * @param ip This is the address to query the data base.
     * @return The asn name.
     */
    private String getAsnName(String ip) {
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        String asnName = null;
        String asnInfo = null;

        if (match.matches()) {
            asnInfo = asn.getOrg(ip);
        } else {
            asnInfo = asn6.getOrgV6(ip);
        }

        if (asnInfo != null) {
            String[] asn = asnInfo.split(" ", 2);

            if (asn.length > 1) {
                if (asn[1] != null) asnName = asn[1];
            } else {
                if (asn[0] != null) asnName = asn[0];
            }
        }
        return asnName;
    }
}
