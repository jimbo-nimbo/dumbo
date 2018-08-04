package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class FetcherFactory {

    private final static String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    private final static Consumer<Long, String> CONSUMER = new KafkaConsumer<>(
            KafkaPropertyFactory.getConsumerProperties());
    private final static Producer<Long, String> PRODUCER = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private final ArrayBlockingQueue queue;

    FetcherFactory(ArrayBlockingQueue queue) {
        this.queue = queue;
        CONSUMER.subscribe(Collections.singletonList(TOPIC));
    }

    public Fetcher newFetcher() {
        return new Fetcher(queue, CONSUMER, PRODUCER);
    }
}
