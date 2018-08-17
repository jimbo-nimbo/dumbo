package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticSearchHandler;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchSetting;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.fetcher.Fetcher;
import ir.sahab.nimbo.jimbo.fetcher.Worker;
import ir.sahab.nimbo.jimbo.hbase.HBaseBulkHandler;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.parser.Parser;
import ir.sahab.nimbo.jimbo.parser.ParserSetting;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {

    private final Shuffler shuffler;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;

    private final Fetcher fetcher;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    private final Parser parser;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;
    private final ArrayBlockingQueue<HBaseDataModel> hbaseQueue;

    private final HBaseBulkHandler hbaseBulkHandler;
    private ElasticSearchHandler elasticSearchHandler;

    public Crawler(CrawlerSetting crawlerSetting) throws Exception {

        shuffledLinksQueue = new ArrayBlockingQueue<>(crawlerSetting.getShuffledQueueMaxSize());
        shuffler = new Shuffler(shuffledLinksQueue);

        elasticQueue = new ArrayBlockingQueue<>(crawlerSetting.getElasticQueueMaxSize());
        try {
            elasticSearchHandler = new ElasticSearchHandler(elasticQueue, new ElasticsearchSetting());
        } catch (UnknownHostException e) {
            //todo
            e.printStackTrace();
            throw new Exception("Cannot connect to elasticsearch");
        }

        rawPagesQueue = new ArrayBlockingQueue<>(crawlerSetting.getRawPagesQueueMaxSize());
        fetcher = new Fetcher(shuffledLinksQueue, rawPagesQueue, new FetcherSetting());

        hbaseQueue = new ArrayBlockingQueue<>(crawlerSetting.getHbaseQueueMaxSize());
        parser = new Parser(rawPagesQueue, elasticQueue, hbaseQueue, new ParserSetting());

        hbaseBulkHandler = new HBaseBulkHandler(hbaseQueue);
    }

    public void crawl() throws InterruptedException {

        new Thread(shuffler).start();

        parser.runWorkers();
        fetcher.runWorkers();
        new Thread(elasticSearchHandler).start();
        new Thread(hbaseBulkHandler).start();

        long uptime = System.currentTimeMillis();
        while (true) {

            System.out.println("shuffled queue: " + shuffledLinksQueue.size()
                    + ",\t fetched queue: " + rawPagesQueue.size()
                    + ", parsedPages" +  Parser.parsedPages.intValue()
                    + ", hbaseQueue: " + hbaseQueue.size()
                    + ", elasticQueue: " + elasticQueue.size()
                    + ",\t uptime: " + (System.currentTimeMillis() - uptime));
            System.out.println("elasticsearch(fail, success): " + ElasticSearchHandler.failureSubmit + ", " +  ElasticSearchHandler.successfulSubmit);
            System.out.println(Worker.log());
            System.out.println("--------------------------------");
            Thread.sleep(10000);
        }
    }

}
