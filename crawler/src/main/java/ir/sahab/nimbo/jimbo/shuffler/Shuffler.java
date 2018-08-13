package ir.sahab.nimbo.jimbo.shuffler;

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

public  class Shuffler implements Runnable{

    private final Consumer<String, String> consumer =  new KafkaConsumer<>(
            KafkaPropertyFactory.getConsumerProperties());

    private final ArrayBlockingQueue<List<String>> linksQueue;


    private boolean running = true;

    /**
     * constructor for testing
     */
    Shuffler(String kafkaTopic, ArrayBlockingQueue<List<String>> linksQueue)
    {
        this.linksQueue = linksQueue;
        consumer.subscribe(Collections.singletonList(kafkaTopic));
    }

    public Shuffler(ArrayBlockingQueue<List<String>> linksQueue)
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
        final List<String> list = new ArrayList<>();
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
        List<String> list;
        List<String> putList = new ArrayList<>();

        while(running) {
            list = consumeAndShuffle();

            try {
                for (int i = 0; i < list.size(); i++) {
                    if (i % 100 == 99 || i == list.size() -1 ) {
                        linksQueue.put(putList);
                        putList = new ArrayList<>();
                    }
                    putList.add(list.get(i));
                }
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

