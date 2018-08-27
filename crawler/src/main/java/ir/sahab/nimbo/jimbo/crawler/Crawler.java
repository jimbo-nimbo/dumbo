package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchHandler;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchSetting;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.fetcher.Fetcher;
import ir.sahab.nimbo.jimbo.hbase.HBaseBulkHandler;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import ir.sahab.nimbo.jimbo.parser.Parser;
import ir.sahab.nimbo.jimbo.parser.ParserSetting;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {
    private static final Logger logger = LoggerFactory.getLogger(Crawler.class);

    private final Shuffler shuffler;
    private final Fetcher fetcher;
    private final Parser parser;
    private final HBaseBulkHandler hbaseBulkHandler;
    private ElasticsearchHandler elasticSearchHandler;

    // TODO: what to do with settings?
    public Crawler(CrawlerSetting crawlerSetting) throws ConnectException {
        ArrayBlockingQueue<List<String>> shuffleQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getShuffledQueueMaxSize());
        ArrayBlockingQueue<WebPageModel> fetchedQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getRawPagesQueueMaxSize());
        ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getElasticQueueMaxSize());
        ArrayBlockingQueue<HBaseDataModel> hbaseQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getHbaseQueueMaxSize());

        shuffler = new Shuffler(shuffleQueue);
        fetcher = new Fetcher(shuffleQueue, fetchedQueue, new FetcherSetting());
        parser = new Parser(fetchedQueue, elasticQueue, hbaseQueue, new ParserSetting());

        hbaseBulkHandler = new HBaseBulkHandler(hbaseQueue);
        try {
            elasticSearchHandler = new ElasticsearchHandler(elasticQueue, new ElasticsearchSetting());
        } catch (UnknownHostException e) {
            logger.error("Could not connect to ElasticSearch");
            throw new ConnectException("Could not connect to ElasticSearch");
        }

        // TODO: isn't it better to use static queues?
        // TODO: static methods? or singleton?
        // TODO: why not use singleton for crawler, as well as parser and fetcher?
        Metrics.getInstance().initialize(shuffleQueue, fetchedQueue, elasticQueue, hbaseQueue);
    }

    public void crawl() {
        new Thread(shuffler).start();
        parser.runWorkers();
        fetcher.runWorkers();
        new Thread(elasticSearchHandler).start();
        new Thread(hbaseBulkHandler).start();

        Metrics.getInstance().startJmxReport();

        Scanner scanner = new Scanner(System.in);
        String cmd;
        do {
            System.out.print("Type 'quit' to exit: ");
            cmd = scanner.next();
        } while (!cmd.equals("quit"));
    }

}
