package io.wizzie.enricher.base.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static String getIdentifier(){
        String identifier;

        try {
            identifier = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
           log.warn("[UnknownHostException] -> Use random identifier");
           identifier = UUID.randomUUID().toString();
        }

        return identifier;
    }
}
