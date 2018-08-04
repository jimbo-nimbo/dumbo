package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FetcherThreadHandler {
    private final static String TOPIC = "TestTopic";

    private static FetcherThreadHandler ourInstance = new FetcherThreadHandler();

    public static FetcherThreadHandler getInstance() { return ourInstance; }


    final static Consumer<Long, String> consumer =
            new KafkaConsumer<>(KafkaPropertyFactory.getConsumerProperties("KafkaExampleConsumer"));

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
