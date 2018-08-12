package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class FetcherManager {

    private final List<Fetcher> fetchers = new ArrayList<>();

    private final LruCache lruCache = LruCache.getInstance();

    private final Producer<Long, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private final ArrayBlockingQueue<String> linksQueue;
    private final ArrayBlockingQueue<String> htmlsQueue;

    public FetcherManager(ArrayBlockingQueue<String> linksQueue, ArrayBlockingQueue<String> htmlsQueue)
    {

        this.linksQueue = linksQueue;
        this.htmlsQueue = htmlsQueue;
    }

    public List<Fetcher> getFetchers() {
        return fetchers;
    }

    public LruCache getLruCache() {
        return lruCache;
    }

    public Producer<Long, String> getProducer() {
        return producer;
    }

    public ArrayBlockingQueue<String> getLinksQueue() {
        return linksQueue;
    }

    public ArrayBlockingQueue<String> getHtmlsQueue() {
        return htmlsQueue;
    }
}
