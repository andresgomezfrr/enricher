package rb.ks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rb.ks.builder.Builder;
import rb.ks.builder.config.Config;

public class Enricher {
    private static final Logger log = LoggerFactory.getLogger(Enricher.class);

    public static void main(String args[]) throws Exception {
        if (args.length == 1) {
            Config config = new Config(args[0]);
            Builder builder = new Builder(config.clone());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                builder.close();
                log.info("Stopped Enricher process.");
            }));


        } else {
            log.error("Execute: java -cp ${JAR_PATH} rb.ks.Enricher <config_file>");
        }

    }
}
