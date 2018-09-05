package ir.sahab.nimbo.jimbo.shuffler;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public  class Shuffler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Shuffler.class);
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

        while (running) {
            List<String> putList = new ArrayList<>(100);
            List<String> list = consumeAndShuffle();

            try {
                for (String link: list) {
                    if (putList.size()>=100) {
                        Metrics.getInstance().markShuffledPacks();
                        linksQueue.put(putList);
                        putList = new ArrayList<>(100);
                    }
                    putList.add(link);
                }
                if (putList.size()>0){
                    Metrics.getInstance().markShuffledPacks();
                    linksQueue.put(putList);
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
        consumer.subscribe(Collections.singletonList(Config.URL_FRONTIER_TOPIC));
        return consumer;
    }
}

