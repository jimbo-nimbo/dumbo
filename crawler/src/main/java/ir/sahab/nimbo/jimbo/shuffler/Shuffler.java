package ir.sahab.nimbo.jimbo.shuffler;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public  class Shuffler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Shuffler.class);

    private final static String TOPIC = Config.URL_FRONTIER_TOPIC;
    final static int MAX_POLL_RECORDS = 200000;

    private final Consumer<String, String> consumer = createConsumer();
    private final ArrayBlockingQueue<List<String>> linksQueue;

    private boolean running = true;

    /**
     * constructor for testing
     */
    Shuffler(String kafkaTopic, ArrayBlockingQueue<List<String>> linksQueue) {
        this.linksQueue = linksQueue;
        consumer.subscribe(Collections.singletonList(kafkaTopic));
    }

    public Shuffler(ArrayBlockingQueue<List<String>> linksQueue) {
        this.linksQueue = linksQueue;
    }

    ConsumerRecords<String, String> consume() {
        Timer.Context kafkaConsumeTimeContext = Metrics.getInstance().kafkaConsumeRequestsTime();
        ConsumerRecords<String, String> consumerRecords = consumer.poll(5000);
        consumer.commitAsync();
        kafkaConsumeTimeContext.stop();
        return consumerRecords;
    }

    List<String> consumeAndShuffle() {
        final List<String> list = new ArrayList<>();
        ConsumerRecords<String, String> consumerRecords = consume();

        for (ConsumerRecord<String, String> consumerRecord: consumerRecords){
            list.add(consumerRecord.value());
        }

        Collections.shuffle(list);
        return list;
    }

    void closeConsumer() {
        running = false;
        consumer.close();
    }

    @Override
    public void run() {
        List<String> list;
        List<String> putList = new ArrayList<>();

        while (running) {
            list = consumeAndShuffle();

            try {
                for (int i = 0; i < list.size(); i++) {
                    // TODO: ask Alireza why this is here?
                    if (linksQueue.remainingCapacity() == 0) {
                        Thread.sleep(10000);
                    }

                    if (i % 100 == 99 || i == list.size() - 1) {
                        Metrics.getInstance().markShuffledPacks();
                        linksQueue.put(putList);
                        putList = new ArrayList<>();
                    }
                    putList.add(list.get(i));

                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private Consumer<String, String> createConsumer() {
        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(KafkaPropertyFactory.getConsumerProperties());
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
}

