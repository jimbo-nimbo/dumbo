package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.fetcher.NewFetcher;
//import ir.sahab.nimbo.jimbo.main.KafkaConsumerExample;
import ir.sahab.nimbo.jimbo.parser.Parser;
import ir.sahab.nimbo.jimbo.parser.ParserSetting;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class NewCrawler {

    private final Shuffler shuffler;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;

    private final NewFetcher fetcher;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    private final Parser parser;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

//    private final ElasticsearchThreadFactory;
    public NewCrawler(CrawlSetting crawlSetting){

        shuffledLinksQueue = new ArrayBlockingQueue<>(crawlSetting.getShuffledQueueMaxSize());
        shuffler = new Shuffler(shuffledLinksQueue);

        rawPagesQueue = new ArrayBlockingQueue<>(crawlSetting.getRawPagesQueueMaxSize());
        //todo: read thread count from properties
        fetcher = new NewFetcher(shuffledLinksQueue, rawPagesQueue, new FetcherSetting(40));

        elasticQueue = new ArrayBlockingQueue<>(crawlSetting.getElasticQueueMaxSize());
        //todo: read thread count from properties
        parser = new Parser(rawPagesQueue, elasticQueue, new ParserSetting(1));
    }

    /**
     * constructor for testing
     */
    NewCrawler(){
        this(new CrawlSetting(10000, 10000, 10000));
    }

    public void crawl() throws InterruptedException {

        new Thread(shuffler).start();

        parser.runWorkers();
        fetcher.runWorkers();

        int tmp = 0;
        int p = 0;
        while(true) {

            p = NewFetcher.fetchedPages.get();
            System.out.println("shuffled links: " + shuffledLinksQueue.size()
                    + ",\t webpages: " + rawPagesQueue.size() + ", " +  Parser.parsedPages.intValue()
                    + " page parsed!\t, lru rate = "
                    + NewFetcher.linkpassed.doubleValue()/
                    (NewFetcher.linkpassed.doubleValue() + NewFetcher.linkNotPassed.doubleValue())
            + " , \t" + (p - tmp) + "page fetched");
            tmp = p;
            Thread.sleep(3000);
        }
    }

}
