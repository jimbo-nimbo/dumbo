package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

class Shuffler implements Runnable{

    private final Consumer<String, String> consumer =  new KafkaConsumer<>(
            KafkaPropertyFactory.getConsumerProperties());

    private final ArrayBlockingQueue<String> linksQueue;

    private final List<String> list = new ArrayList<>();

    private boolean running = true;

    /**
     * constructor for testing
     */
    Shuffler(String kafkaTopic, ArrayBlockingQueue<String> linksQueue)
    {
        this.linksQueue = linksQueue;
        consumer.subscribe(Collections.singletonList(kafkaTopic));
    }

    Shuffler(ArrayBlockingQueue<String> linksQueue)
    {
        this.linksQueue = linksQueue;
        consumer.subscribe(Collections.singletonList(Config.URL_FRONTIER_TOPIC));;
    }

    ConsumerRecords<String, String> consume()
    {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(5000);
        consumer.commitAsync();
        return consumerRecords;
    }

    List<String> consumeAndShuffle()
    {
        list.clear();
        ConsumerRecords<String, String> consumerRecords = consume();

        for (ConsumerRecord<String, String> consumerRecord: consumerRecords){
            list.add(consumerRecord.value());
        }

        Collections.shuffle(list);
        return list;
    }

    void closeConsumer()
    {
        running = false;
        consumer.close();
    }

    @Override
    public void run() {
        List<String> list = new ArrayList<>();

        while(running) {
            if (list.size() <= 0 ) {
                list = consumeAndShuffle();
            } else {
                try {
                    linksQueue.put(list.remove(0));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
