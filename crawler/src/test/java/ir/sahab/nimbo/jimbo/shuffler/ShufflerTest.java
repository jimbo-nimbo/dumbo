package ir.sahab.nimbo.jimbo.shuffler;

import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ShufflerTest {

    private String kafkaTopic = "ShuffleTopicTest";

    private int maxPollSize = Shuffler.MAX_POLL_RECORDS;

    private int linksQueueSize = 10000;

    private String testValue1 = "-";
    private String testValue2 = "_";

    @Test
    public void consume() throws InterruptedException {

        Producer<String, String> producer = new KafkaProducer<>(
                KafkaPropertyFactory.getProducerProperties());

        producer.send(new ProducerRecord<>(kafkaTopic, null, testValue1));

        Thread.sleep(5000);

        ArrayBlockingQueue<List<String>> linksQueue = new ArrayBlockingQueue<>(linksQueueSize);
        Shuffler shuffler = new Shuffler(kafkaTopic, linksQueue);
        ConsumerRecords<String, String> messages = shuffler.consume();

        assertEquals(1, messages.count());
        for (ConsumerRecord<String, String> message: messages){
            assertEquals(testValue1, message.value());
        }

        shuffler.closeConsumer();
        producer.close();
    }

    @Test
    public void testMaxPollSize()
    {
        Producer<String, String> producer = new KafkaProducer<>(
                KafkaPropertyFactory.getProducerProperties());

        for (long i = 0; i < maxPollSize + 100; i++) {
            producer.send(new ProducerRecord<>(kafkaTopic, null, testValue1));
        }

        ArrayBlockingQueue<List<String>> linksQueue = new ArrayBlockingQueue<>(linksQueueSize);
        Shuffler shuffler = new Shuffler(kafkaTopic, linksQueue);
        ConsumerRecords<String, String> messages = shuffler.consume();

        assertEquals(maxPollSize, messages.count());
        for (ConsumerRecord<String, String> message: messages){
            assertEquals(testValue1, message.value());
        }

        shuffler.closeConsumer();
        producer.close();
    }

    @Test
    public void testShuffle()
    {
        Producer<String, String> producer = new KafkaProducer<>(
                KafkaPropertyFactory.getProducerProperties());

        for (long i = 0; i < maxPollSize/2; i++) {
            producer.send(new ProducerRecord<>(kafkaTopic, null, testValue1));
            producer.send(new ProducerRecord<>(kafkaTopic, null, testValue2));
        }

        ArrayBlockingQueue<List<String>> linksQueue = new ArrayBlockingQueue<>(linksQueueSize);
        Shuffler shuffler = new Shuffler(kafkaTopic, linksQueue);
        List<String> list = shuffler.consumeAndShuffle();

        for (int i = 0; i < maxPollSize/2; i++) {
            if (!list.get(i).equals(testValue1)) {
                assertTrue(true);
                return;
            }
        }

        for (int i = maxPollSize/2; i < maxPollSize; i++) {
            if (!list.get(i).equals(testValue2)) {
                assertTrue(true);
                return;
            }
        }
        shuffler.closeConsumer();
        producer.close();

        fail();
    }

    @Test
    public void linksQueueTest() throws InterruptedException {

        ArrayBlockingQueue<List<String>> links = new ArrayBlockingQueue<>(linksQueueSize);

        Shuffler shuffler = new Shuffler(kafkaTopic, links);
        new Thread(shuffler).start();

        Producer<String, String> producer = new KafkaProducer<>(
                KafkaPropertyFactory.getProducerProperties());

        for (long i = 0; i < maxPollSize/2; i++) {
            producer.send(new ProducerRecord<>(kafkaTopic, null, testValue1));
            producer.send(new ProducerRecord<>(kafkaTopic, null, testValue2));
        }

        for (int i = 0; i < maxPollSize / 2; i++) {
            if (!links.take().equals(testValue1)) {
                assertTrue(true);
                return;
            }
        }
        shuffler.closeConsumer();
        producer.close();

        fail();
    }

    @Before
    public void clearKafka()
    {
        boolean b = true;
        Consumer<String, String> consumer =  new KafkaConsumer<>(
                KafkaPropertyFactory.getConsumerProperties());
        consumer.subscribe(Collections.singletonList(kafkaTopic));
        while(b){
            ConsumerRecords<String, String> poll = consumer.poll(500);
            consumer.commitSync();
            if (poll.count() == 0) {
                b = false;
            }
        }

        consumer.close();
    }
}