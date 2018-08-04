package ir.sahab.nimbo.jimbo;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class KafkaConsumerExample {
    private final static String TOPIC = "TutorialTopic";

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(KafkaPropertyFactory.getConsumerProperties());
        consumer.subscribe(Collections.singletonList(TOPIC));

        final int giveUp = 100;
        int noRecordsCount = 0;
        while (noRecordsCount < giveUp) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0)
                noRecordsCount++;
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }
}
