package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.fetcher.NewFetcher;
import ir.sahab.nimbo.jimbo.parser.Parser;
import ir.sahab.nimbo.jimbo.parser.ParserSetting;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {

    private final Shuffler shuffler;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;

    private final NewFetcher fetcher;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    private final Parser parser;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    public Crawler(CrawlerSetting crawlerSetting) {

        shuffledLinksQueue = new ArrayBlockingQueue<>(crawlerSetting.getShuffledQueueMaxSize());
        shuffler = new Shuffler(shuffledLinksQueue);

        rawPagesQueue = new ArrayBlockingQueue<>(crawlerSetting.getRawPagesQueueMaxSize());
        fetcher = new NewFetcher(shuffledLinksQueue, rawPagesQueue, new FetcherSetting());

        elasticQueue = new ArrayBlockingQueue<>(crawlerSetting.getElasticQueueMaxSize());
        parser = new Parser(rawPagesQueue, elasticQueue, new ParserSetting());
    }

    /**
     * constructor for testing
     */
    Crawler() {
        this(new CrawlerSetting(10000, 10000, 10000));
    }

    public void crawl() throws InterruptedException {

        new Thread(shuffler).start();

        parser.runWorkers();
        fetcher.runWorkers();

        int tmp = 0;
        int p;
        while (true) {

            p = NewFetcher.fetchedPages.get();
            System.out.println("shuffled links: " + shuffledLinksQueue.size()
                    + ",\t webpages: " + rawPagesQueue.size() + ", " +  Parser.parsedPages.intValue()
                    + " page parsed!\t, lru rate = "
                    + NewFetcher.linkpassed.doubleValue()/
                    (NewFetcher.linkpassed.doubleValue() + NewFetcher.linkNotPassed.doubleValue())
            + " , \t>" + (p - tmp) + "page fetched <");
            tmp = p;
            Thread.sleep(3000);
        }
    }

}
