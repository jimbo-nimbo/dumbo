package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.MalformedURLException;
import java.util.Collections;

public class FetcherThreadHandler {

    private final static String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    private static Consumer<Long, String> consumer;

    private static FetcherThreadHandler ourInstance;


    public synchronized static FetcherThreadHandler getInstance() {
        if (ourInstance == null)
            ourInstance = new FetcherThreadHandler();
        return ourInstance;
    }

    private FetcherThreadHandler()
    {
        consumer =
                new KafkaConsumer<>(KafkaPropertyFactory.getConsumerProperties("KafkaExampleConsumer"));
    }



    private static void runConsumer() {
        consumer.subscribe(Collections.singletonList(TOPIC));

        final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10000);

        consumerRecords.forEach(record -> {
            try {
                new FetcherThread(record.value()).run();
            } catch (MalformedURLException m) {
                m.printStackTrace();
            }
        });

        consumer.commitAsync();
        consumer.close();
    }

}
