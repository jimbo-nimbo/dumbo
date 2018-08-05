package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaTopics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Scanner;

class Seeder {

    private static final String SEED_NAME = "top-200.csv";
    private static Seeder seeder = null;
    private Scanner inp;

    private Seeder() {
        ClassLoader classLoader = getClass().getClassLoader();
        inp = new Scanner((classLoader.getResourceAsStream(SEED_NAME)));
    }

    synchronized static Seeder getInstance() {
        if (seeder == null)
            seeder = new Seeder();
        return seeder;
    }

    void initializeKafka() {
        final Producer<Long, String> producer = new KafkaProducer<>(
                KafkaPropertyFactory.getProducerProperties());
        while (inp.hasNext()) {
            String url = "https://www." + inp.next();
            ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaTopics.URL_FRONTIER.toString(), null,
                    url);
            producer.send(record);
        }
    }

}
