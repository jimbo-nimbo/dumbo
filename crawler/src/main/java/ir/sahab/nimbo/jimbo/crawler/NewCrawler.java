package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.fetcher.NewFetcher;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;

import java.util.concurrent.ArrayBlockingQueue;

public class NewCrawler {

    private final Shuffler shuffler;
    private final ArrayBlockingQueue<String> shuffledLinksQueue;

    private final NewFetcher fetcher;
    private final ArrayBlockingQueue<String> rawPagesQueue;

    public NewCrawler(CrawlSetting crawlSetting){

        shuffledLinksQueue = new ArrayBlockingQueue<>(crawlSetting.getShuffledQueueMaxSize());
        shuffler = new Shuffler(shuffledLinksQueue);

        rawPagesQueue = new ArrayBlockingQueue<>(crawlSetting.getRawPagesQueueMaxSize());
        //todo: read thread count from properties
        fetcher = new NewFetcher(shuffledLinksQueue, rawPagesQueue, new FetcherSetting(200));


    }

    /**
     * constructor for testing
     */
    NewCrawler(){
        this(new CrawlSetting(10000, 10000));
    }
}
