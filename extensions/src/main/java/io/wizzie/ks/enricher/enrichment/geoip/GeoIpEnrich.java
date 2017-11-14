package io.wizzie.ks.enricher.enrichment.geoip;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import io.wizzie.ks.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.ks.enricher.enrichment.utils.Constants;
import io.wizzie.metrics.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static io.wizzie.ks.enricher.enrichment.utils.Constants.*;

public class GeoIpEnrich extends BaseEnrich {
    private static final Logger log = LoggerFactory.getLogger(GeoIpEnrich.class);
    String SRC_COUNTRY_CODE = "src_country_code";
    String DST_COUNTRY_CODE = "dst_country_code";
    String SRC_CITY = "src_city";
    String DST_CITY = "dst_city";
    String SRC_IP = "src";
    String DST_IP = "dst";
    String SRC_AS_NAME = "src_as_name";
    String DST_AS_NAME = "dst_as_name";
    String SRC_LATITUDE = "src_latitude";
    String SRC_LONGITUDE = "src_longitude";
    String DST_LATITUDE = "dst_latitude";
    String DST_LONGITUDE = "dst_longitude";

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
        SRC_COUNTRY_CODE = (String) properties.getOrDefault(SRC_COUNTRY_CODE_DIM, "src_country_code");
        DST_COUNTRY_CODE = (String) properties.getOrDefault(DST_COUNTRY_CODE_DIM, "dst_country_code");
        SRC_CITY = (String) properties.getOrDefault(SRC_CITY_DIM, "src_city");
        DST_CITY = (String) properties.getOrDefault(DST_CITY_DIM, "dst_city");
        SRC_IP = (String) properties.getOrDefault(SRC_DIM, "src");
        DST_IP = (String) properties.getOrDefault(DST_DIM, "dst");
        SRC_AS_NAME = (String) properties.getOrDefault(SRC_AS_NAME_DIM, "src_as_name");
        DST_AS_NAME = (String) properties.getOrDefault(DST_AS_NAME_DIM, "dst_as_name");
        SRC_LATITUDE = (String) properties.getOrDefault(SRC_LATITUDE_DIM, "src_latitude");
        SRC_LONGITUDE = (String) properties.getOrDefault(SRC_LONGITUDE_DIM, "src_longitude");
        DST_LATITUDE = (String) properties.getOrDefault(DST_LATITUDE_DIM, "dst_latitude");
        DST_LONGITUDE = (String) properties.getOrDefault(DST_LONGITUDE_DIM, "dst_longitude");

        String ASN_V6_DB_PATH = (String) properties.getOrDefault(ASN6_DB_PATH, "/opt/share/GeoIP/asnv6.dat");
        String ASN_DB_PATH = (String) properties.getOrDefault(Constants.ASN_DB_PATH, "/opt/share/GeoIP/asn.dat");
        String CITY_V6_DB_PATH = (String) properties.getOrDefault(CITY6_DB_PATH, "/opt/share/GeoIP/cityv6.dat");
        String CITY_DB_PATH = (String) properties.getOrDefault(Constants.CITY_DB_PATH, "/opt/share/GeoIP/city.dat");

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

        Map<String, Object> srcData = getDataByIpAndSite(src, SRC_COUNTRY_CODE, SRC_CITY, SRC_AS_NAME, SRC_LATITUDE,
                SRC_LONGITUDE);
        Map<String, Object> dstData = getDataByIpAndSite(dst, DST_COUNTRY_CODE, DST_CITY, DST_AS_NAME, DST_LATITUDE,
                DST_LONGITUDE);

        if(srcData != null) geoIPMap.putAll(srcData);
        if(dstData != null) geoIPMap.putAll(dstData);


        return geoIPMap;
    }

    public Map<String, Object> getDataByIpAndSite(String ip, String countryCodeDim, String cityDim, String asNameDim,
    String latidudeDim, String longitudeDim){
        Map<String, Object> geoIPMap = null;

        if (ip != null) {
            geoIPMap = new HashMap<>();
            Map<String, Object> locations = new HashMap<>();
            String asn_name = null;

            if (VALID_IPV4_PATTERN != null) {
                locations = getCountryCode(ip);
                asn_name = getAsnName(ip);
            }

            String countryCode = (String) locations.get("country_code");
            if (countryCode != null) geoIPMap.put(countryCodeDim, countryCode);

            String city = (String) locations.get("city");
            if (city != null) geoIPMap.put(cityDim, city);

            if (asn_name != null) geoIPMap.put(asNameDim, asn_name);

            Float latitude = (Float) locations.get("latitude");
            if(latitude != null ) geoIPMap.put(latidudeDim, latitude);

            Float longitude = (Float) locations.get("longitude");
            if(longitude != null ) geoIPMap.put(longitudeDim, longitude);
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
    private Map<String, Object> getCountryCode(String ip) {
        Map<String, Object> locations = new HashMap<>();
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        Location location;

        if (match.matches()) {
            location = city.getLocation(ip);
        } else {
            location = city6.getLocationV6(ip);
        }

        if (location != null) {
            locations.put("latitude", location.latitude);
            locations.put("longitude", location.longitude);
            locations.put("country_code", location.countryCode);
            locations.put("city", location.city);
        }

        return locations;
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
