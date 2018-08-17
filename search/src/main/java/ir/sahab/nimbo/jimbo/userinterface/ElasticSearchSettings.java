package ir.sahab.nimbo.jimbo.userinterface;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ElasticSearchSettings {

    protected final Properties properties;
    private final String bootstrapServers;
    private final String producerClientId;
    private final String consumerGroupId;
    private final int maxPollRecords;

    ElasticSearchSettings() {
        String resourceName = "elasticsearch.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            properties.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        bootstrapServers = properties.getProperty("bootstrap_servers");
        producerClientId = properties.getProperty("producer_client_id");
        consumerGroupId = properties.getProperty("consumer_group_id");
        maxPollRecords = Integer.parseInt(properties.getProperty("max_poll_records"));
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getProducerClientId() {
        return producerClientId;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }


}
