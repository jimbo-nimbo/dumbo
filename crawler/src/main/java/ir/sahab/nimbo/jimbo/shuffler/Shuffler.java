package ir.sahab.nimbo.jimbo.shuffler;

import ir.sahab.nimbo.jimbo.main.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public  class Shuffler implements Runnable{

    private final static String TOPIC = Config.URL_FRONTIER_TOPIC;
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
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
//        consumer.subscribe(Collections.singletonList(Config.URL_FRONTIER_TOPIC));;
    }

    ConsumerRecords<String, String> consume() {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(5000);
        consumer.commitAsync();
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
//	System.out.println("shuffler started!");
//            runConsumer();

        List<String> list;
        List<String> putList = new ArrayList<>();

        while(running) {
            list = consumeAndShuffle();

            try {
                for (int i = 0; i < list.size(); i++) {
                    if (linksQueue.remainingCapacity() == 0) {
                        Thread.sleep(10000);
                    }

                    if (i % 100 == 99 || i == list.size() -1 ) {
                        linksQueue.put(putList);
                        putList = new ArrayList<>();
                    }
                    putList.add(list.get(i));

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaShuffleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;

    }

    public void runConsumer() {
        System.out.println("example! ");
        final Consumer<String, String> consumer = createConsumer();
        final int giveUp = 1000;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    break;
                else
                    continue;
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}

