package rb.ks.enrichment;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static rb.ks.enrichment.utils.Constants.__CLIENT_MAC;
import static rb.ks.enrichment.utils.Constants.__CLIENT_MAC_VENDOR;

public class MacVendorEnrichUnitTest {

    @Test
    public void enrichesWithMacVendor() {
        // Enriches when the MAC is found
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();

        Map<String, Object> properties = new HashMap<>();
        properties.put("oui.file.path", ClassLoader.getSystemResource("mac_vendors").getFile());

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
    public void logsWhenVendorFileNotFound() {
        MacVendorEnrich macVendorEnrich = new MacVendorEnrich();

        Map<String, Object> properties = new HashMap<>();
        properties.put("oui.file.path",  "/this_path_doesnt_exist");

        macVendorEnrich.init(properties, null);

        assertTrue(macVendorEnrich.ouiMap.isEmpty());
    }
}
