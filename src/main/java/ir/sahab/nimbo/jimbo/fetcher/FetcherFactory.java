package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class FetcherFactory {

    private final static String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    private final static Consumer<Long, String> CONSUMER =new KafkaConsumer<>(
            KafkaPropertyFactory.getConsumerProperties());

    private static ArrayBlockingQueue queue;

    public FetcherFactory(ArrayBlockingQueue queue) {
        this.queue = queue;
        CONSUMER.subscribe(Collections.singletonList(TOPIC));
    }

    public Fetcher newFetcher() {
        return new Fetcher(queue, CONSUMER);
    }
}
