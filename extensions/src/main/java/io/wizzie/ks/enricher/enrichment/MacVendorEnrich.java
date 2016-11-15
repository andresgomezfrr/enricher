package io.wizzie.ks.enricher.enrichment;

import io.wizzie.ks.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.ks.enricher.metrics.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static io.wizzie.ks.enricher.enrichment.utils.Constants.__CLIENT_MAC;
import static io.wizzie.ks.enricher.enrichment.utils.Constants.__CLIENT_MAC_VENDOR;

public class MacVendorEnrich extends BaseEnrich {

    private static final Logger log = LoggerFactory.getLogger(MacVendorEnrich.class);

    public static String ouiFilePath;
    public Map<String, String> ouiMap;

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {

        ouiFilePath = String.valueOf(properties.getOrDefault("oui.file.path", "/opt/etc/objects/mac_vendors"));
        ouiMap = new HashMap<>();

        InputStream in = null;

        try {
            in = new FileInputStream(ouiFilePath);
        } catch (FileNotFoundException e) {
            log.error("The MacVendor file couldn't be found", e);
        }

        if(in != null) {
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(isr);

            try {
                String line = br.readLine();

                while(line != null) {
                    String[] tokens = line.split("\\|");

                    if(tokens.length == 2)
                        ouiMap.put(tokens[0], tokens[1]);

                    line = br.readLine();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> vendorMap = new HashMap<>();
        vendorMap.putAll(message);

        String clientMac = String.valueOf(message.get(__CLIENT_MAC));

        if (clientMac != null) {
            String oui = buildOui(clientMac);

            if (ouiMap.get(oui) != null) {
                vendorMap.put(__CLIENT_MAC_VENDOR, ouiMap.get(oui));
            }

        }

        return vendorMap;
    }

    private String buildOui(Object object) {
        String mac = object.toString();
        mac = mac.trim().replaceAll("[-:]", "");
        return mac.substring(0, 6).toUpperCase();
    }

    @Override
    public void stop() {

    }
}
