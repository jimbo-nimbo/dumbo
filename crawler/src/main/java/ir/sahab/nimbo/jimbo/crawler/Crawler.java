package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchHandler;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchSetting;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.Fetcher;
import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.hbase.HBaseBulkDataHandler;
import ir.sahab.nimbo.jimbo.hbase.HBaseBulkMarkHandler;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import ir.sahab.nimbo.jimbo.parser.Parser;
import ir.sahab.nimbo.jimbo.parser.ParserSetting;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Crawler {
    private static final Logger logger = LoggerFactory.getLogger(Crawler.class);

    private final Shuffler shuffler;
    private final Fetcher fetcher;
    private final Parser parser;
    private final HBaseBulkDataHandler hbaseBulkDataHandler;
    private final HBaseBulkMarkHandler hBaseBulkMarkHandler;
    private ElasticsearchHandler elasticSearchHandler;
    private AtomicInteger parserThreadControler = new AtomicInteger(8);

    // TODO: what to do with settings?
    public Crawler(CrawlerSetting crawlerSetting){
        ArrayBlockingQueue<List<String>> shuffleQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getShuffledQueueMaxSize());
        ArrayBlockingQueue<WebPageModel> fetchedQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getRawPagesQueueMaxSize());
        ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getElasticQueueMaxSize());
        ArrayBlockingQueue<HBaseDataModel> hbaseDataQueue = new
                ArrayBlockingQueue<>(crawlerSetting.getHbaseQueueMaxSize());


        shuffler = new Shuffler(shuffleQueue);
        fetcher = new Fetcher(shuffleQueue, fetchedQueue, new FetcherSetting());
        parser = new Parser(fetchedQueue, elasticQueue, hbaseDataQueue, new ParserSetting(), parserThreadControler);

        hbaseBulkDataHandler = new HBaseBulkDataHandler(hbaseDataQueue);
        elasticSearchHandler = new ElasticsearchHandler(elasticQueue, new ElasticsearchSetting());
        hBaseBulkMarkHandler = new HBaseBulkMarkHandler();
        // TODO: isn't it better to use static queues?
        // TODO: static methods? or singleton?
        // TODO: why not use singleton for crawler, as well as parser and fetcher?
        Metrics.getInstance().initialize(shuffleQueue, fetchedQueue, elasticQueue, hbaseDataQueue);
    }

    public void crawl() throws ConnectException {
        new Thread(shuffler).start();
        parser.runWorkers();
        fetcher.runWorkers();
        elasticSearchHandler.runWorkers();
        hBaseBulkMarkHandler.runWorkers();
        hbaseBulkDataHandler.runWorkers();

        Metrics.getInstance().startJmxReport();
        Metrics.getInstance().startCsvReport();

        Scanner scanner = new Scanner(System.in);
        String cmd;
        do {
            System.out.print("write number of core to run");
            cmd = scanner.next();
            try {
                parserThreadControler.set(Integer.valueOf(cmd));
            } catch (Exception e){
                logger.error("wrong number format in crawler input scanner");
            }

        } while (!cmd.equals("quit"));
    }

}
