package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaTopics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.MalformedURLException;
import java.util.Collections;

public class FetcherThreadHandler {

    private final static String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    private final static Consumer<Long, String> CONSUMER =new KafkaConsumer<>(
            KafkaPropertyFactory.getConsumerProperties());

    private static FetcherThreadHandler ourInstance;


    public synchronized static FetcherThreadHandler getInstance() {
        if (ourInstance == null)
            ourInstance = new FetcherThreadHandler();
        return ourInstance;
    }

    private FetcherThreadHandler() {
    }

    private static void runConsumer() {
        CONSUMER.subscribe(Collections.singletonList(TOPIC));

        final ConsumerRecords<Long, String> consumerRecords = CONSUMER.poll(10000);

        consumerRecords.forEach(record -> {
            try {
                new FetcherThread(record.value()).run();
            } catch (MalformedURLException m) {
                m.printStackTrace();
            }
        });

        CONSUMER.commitAsync();
        CONSUMER.close();
    }

}
