package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.MalformedURLException;
import java.util.Collections;

public class FetcherThreadHandler {
    private final static String TOPIC = "TestTopic";

    private static FetcherThreadHandler ourInstance = new FetcherThreadHandler();

    public static FetcherThreadHandler getInstance() { return ourInstance; }


    final static Consumer<Long, String> consumer =
            new KafkaConsumer<>(KafkaPropertyFactory.getConsumerProperties());

    private static void runConsumer() {
        consumer.subscribe(Collections.singletonList(TOPIC));
        final int giveUp = 100;
        int noRecordsCount = 0;
        while (noRecordsCount < giveUp) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10000);
            if (consumerRecords.count() == 0)
                noRecordsCount++;
            consumerRecords.forEach(record -> {
                try {
                    new FetcherThread(record.value()).run();
                } catch (MalformedURLException m) {
                    m.printStackTrace();
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
    }

    public static void main(String[] args) {
        runConsumer();
    }
}
