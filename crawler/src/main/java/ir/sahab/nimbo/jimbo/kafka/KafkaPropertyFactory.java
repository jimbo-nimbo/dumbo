package ir.sahab.nimbo.jimbo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaPropertyFactory {

    private static AtomicInteger producerId = new AtomicInteger(0);
    private static KafkaSetting kafkaSetting;

    static {
        kafkaSetting = new KafkaSetting();
    }

    public static Properties getProducerProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaSetting.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                kafkaSetting.getProducerClientId() + producerId.getAndIncrement());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return props;
    }

    public static Properties getConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaSetting.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaSetting.getConsumerGroupId());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                kafkaSetting.getMaxPollRecords());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        return props;
    }
}
