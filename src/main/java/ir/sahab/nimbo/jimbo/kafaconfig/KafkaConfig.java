package ir.sahab.nimbo.jimbo.kafaconfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS;
    public static final String PRODUCER_CLIENT_ID;
    public static final String CONSUMER_GROUP_ID;

    static {
        String resourceName = "kafka.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)){
            props.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BOOTSTRAP_SERVERS = props.getProperty("bootstrap_servers");
        PRODUCER_CLIENT_ID = props.getProperty("producer_client_id");
        CONSUMER_GROUP_ID = props.getProperty("consumer_group_id");
    }
}
