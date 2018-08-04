package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FetcherKafkaProducer {

    private static FetcherKafkaProducer ourInstance;

    private final static String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    public synchronized static FetcherKafkaProducer getInstance() {
        if (ourInstance == null)
            ourInstance = new FetcherKafkaProducer() ;
        return ourInstance;
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer =
                new KafkaProducer<>(KafkaPropertyFactory.getProducerProperties());
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, null,
                                "Hello Mom (distribute to partitions) " + index);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) " +
                                        "meta(partition=%d, offset=%d) time=%d\n",
                                record.key(), record.value(), metadata.partition(),
                                metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }
    }

}
