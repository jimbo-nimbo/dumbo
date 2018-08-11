package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.elasticSearch.ElasticCannotLoadException;
import ir.sahab.nimbo.jimbo.elasticSearch.ElasticsearchThreadFactory;
import ir.sahab.nimbo.jimbo.elasticSearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.Fetcher;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.PageExtractor;
import ir.sahab.nimbo.jimbo.parser.PageExtractorFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {

    private static final int FETCHER_NUMBER = Config.FETCHER_THREAD_NUM;
    private static final int CONSUMER_NUMBER = Config.CONSUMER_NUMBER;
    private static final int PARSER_NUMBER = Config.PARSER_THREAD_NUM;
    private static final int MAX_DOCUMENT = Config.BLOCKING_QUEUE_SIZE;

    //todo: read from config file
    private static final int ELASTIC_QUEUE_MAX_SIZE = 10000;

    private final static String TOPIC = Config.URL_FRONTIER_TOPIC;

    private final ArrayBlockingQueue<Document> queue;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticsearchWebpageModels;

    private final Consumer<Long, String>[] consumers;
    private final Runnable[] fetchers;
    private final Runnable[] parsers;

    //todo: create more than one thread
    private Runnable elasticsearchThread;

    private Consumer<Long, String> consumerDistributor(int fetcherNum) {
        return consumers[fetcherNum * CONSUMER_NUMBER / FETCHER_NUMBER];
    }

    public Crawler() {

        queue = new ArrayBlockingQueue<>(MAX_DOCUMENT);


        consumers = new Consumer[CONSUMER_NUMBER];
        create_Consumers();
        elasticsearchWebpageModels = new ArrayBlockingQueue<>(ELASTIC_QUEUE_MAX_SIZE);

        final PageExtractorFactory pageExtractorFactory = new PageExtractorFactory(queue);

        fetchers = new Runnable[FETCHER_NUMBER];
        for (int i = 0; i < FETCHER_NUMBER; i++) {
            fetchers[i] = new Fetcher(queue, consumerDistributor(i), TOPIC);
        }

        parsers = new Runnable[PARSER_NUMBER];
        create_Parsers();
    }

    void create_Consumers() {
        for(int i = 0; i < CONSUMER_NUMBER; i++)
            consumers[i] = new KafkaConsumer<>(
                    KafkaPropertyFactory.getConsumerProperties());
    }

    void create_Parsers() {
        final PageExtractorFactory pageExtractorFactory = new PageExtractorFactory(queue);
        for (int i = 0; i < PARSER_NUMBER; i++) {
            parsers[i] = pageExtractorFactory.getPageExtractor();
        }
    }
    /**
     * start threads
     * and log data every 1 second
     */
    public void run() throws ElasticCannotLoadException, IOException {
        for (int i = 0; i < FETCHER_NUMBER; i++) {
            new Thread(fetchers[i]).start();
        }

        for (int i = 0; i < PARSER_NUMBER; i++) {
            new Thread(parsers[i]).start();
        }

        ElasticsearchThreadFactory elasticsearchThreadFactory =
                new ElasticsearchThreadFactory(elasticsearchWebpageModels);
        new Thread(elasticsearchThreadFactory.createNewThread()).start();

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(PageExtractor.pageCounter.get() + " queue size : " + queue.size());
            PageExtractor.pageCounter.set(0);

        }
    }

}
