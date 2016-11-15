package io.wizzie.ks.builder.bootstrap;

import io.wizzie.ks.builder.Builder;
import io.wizzie.ks.builder.config.Config;
import io.wizzie.ks.metrics.MetricsManager;

import java.io.BufferedReader;
import java.io.FileReader;

public class FileBootstraper extends ThreadBootstraper {
    public static final String FILE_PATH = "file.bootstraper.path";
    @Override
    public void run() {
        //Nothing to do
    }

    @Override
    public void init(Builder builder, Config config, MetricsManager metricsManager) throws Exception {
        String filePath = config.get(FILE_PATH);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));

        StringBuilder stringBuffer = new StringBuilder();
        String line;

        while ((line = bufferedReader.readLine()) != null) {

            stringBuffer.append(line).append("\n");
        }

        builder.updateStreamConfig(stringBuffer.toString());
    }

    @Override
    public void close() {
        //Nothing to do
    }
}
