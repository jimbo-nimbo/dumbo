package ir.sahab.nimbo.jimbo.kafaconfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaPropertyFactory {
    private static final String BOOTSTRAP_SERVERS;
    private static final String PRODUCER_CLIENT_ID;
    private static final String CONSUMER_GROUP_ID;
    private static final int MAX_POLL_RECORDS;

    static {
        String resourceName = "kafka.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BOOTSTRAP_SERVERS = props.getProperty("bootstrap_servers");
        PRODUCER_CLIENT_ID = props.getProperty("producer_client_id");
        CONSUMER_GROUP_ID = props.getProperty("consumer_group_id");
        MAX_POLL_RECORDS = Integer.valueOf(props.getProperty("max_poll_records"));
    }

    private static AtomicInteger id = new AtomicInteger(0);

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                PRODUCER_CLIENT_ID + id.getAndIncrement());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return props;
    }

    public static Properties getConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return props;
    }
}
