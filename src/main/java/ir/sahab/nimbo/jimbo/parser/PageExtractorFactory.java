package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.jsoup.nodes.Document;

import java.util.concurrent.ArrayBlockingQueue;

public class PageExtractorFactory {
    private static final Producer<Long, String> PRODUCER = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());
    private final ArrayBlockingQueue<Document> queue;

    public PageExtractorFactory(ArrayBlockingQueue<Document> queue) {
        this.queue = queue;
    }

    public PageExtractor getPageExtractor() {
        return new PageExtractor(PRODUCER, queue);
    }
}
