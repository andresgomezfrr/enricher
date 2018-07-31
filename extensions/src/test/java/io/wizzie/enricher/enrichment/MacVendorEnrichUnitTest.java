package io.wizzie.enricher.enrichment;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.wizzie.enricher.enrichment.utils.Constants.*;
import static org.junit.Assert.*;

public class MacVendorEnrichUnitTest {

    private final static String __CLIENT_MAC = "client_mac";
    private final static String __CLIENT_MAC_VENDOR = "mac_vendor";

    @Test
    public void enrichesWithMacVendor() {
        // Enriches when the MAC is found
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();

        Map<String, Object> properties = new HashMap<>();
        properties.put(OUI_FILE_PATH, ClassLoader.getSystemResource("mac_vendors").getFile());
        properties.put(MAC_DIM, "client_mac");
        properties.put(MAC_VENDOR_DIM, "mac_vendor");

        macVendorEnrich.init(properties, null);

        Map<String, Object> messageApple = new HashMap<>();
        messageApple.put(__CLIENT_MAC, "00:1C:B3:09:85:15");

        Map<String, Object> enriched = macVendorEnrich.enrich(messageApple);
        assertEquals("Apple", enriched.get(__CLIENT_MAC_VENDOR));

        // It doesn't define CLIENT_MAC_VENDOR field when the MAC is not found
        Map<String, Object> messageWithoutVendor = new HashMap<>();
        messageWithoutVendor.put(__CLIENT_MAC, "AA:AA:AA:AA:AA:AA");

        Map<String, Object> enrichedWithoutVendor = macVendorEnrich.enrich(messageWithoutVendor);
        assertNull(enrichedWithoutVendor.get(__CLIENT_MAC_VENDOR));
    }

    @Test
    public void defaultPropertiesShouldWorkCorrectly() {
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();
        Map<String, Object> properties = new HashMap<>();

        macVendorEnrich.init(properties, null);
        properties.put(OUI_FILE_PATH, "path_to_oui_file");
        properties.put(MAC_DIM, "my_mac_field");
        properties.put(MAC_VENDOR_DIM, "my_mac_vendor_field");

        macVendorEnrich.init(properties, null);

        assertEquals("my_mac_field", macVendorEnrich.mac);
        assertEquals("my_mac_vendor_field", macVendorEnrich.macVendor);
        assertEquals("path_to_oui_file", macVendorEnrich.ouiFilePath);
    }

    @Test
    public void dimensionNameShouldBeCorrectly() {
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();
        Map<String, Object> properties = new HashMap<>();

        macVendorEnrich.init(properties, null);

        assertEquals("mac", macVendorEnrich.mac);
        assertEquals("mac_vendor", macVendorEnrich.macVendor);
        assertEquals("/opt/etc/objects/mac_vendors", macVendorEnrich.ouiFilePath);
    }

    @Test
    public void logsWhenVendorFileNotFound() {
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();

        Map<String, Object> properties = new HashMap<>();
        properties.put(OUI_FILE_PATH,  "/this_path_doesnt_exist");

        macVendorEnrich.init(properties, null);

        assertTrue(macVendorEnrich.ouiMap.isEmpty());
    }
}
