package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.fetcher.FetcherSetting;
import ir.sahab.nimbo.jimbo.fetcher.NewFetcher;
import ir.sahab.nimbo.jimbo.hbase.HbaseBulkThread;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import ir.sahab.nimbo.jimbo.shuffler.Shuffler;
import org.apache.hadoop.hbase.client.Put;

import java.util.concurrent.ArrayBlockingQueue;

public class NewCrawler {

    private final Shuffler shuffler;
    private final ArrayBlockingQueue<String> shuffledLinksQueue;

    private final NewFetcher fetcher;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

//    private final HbaseBulkThread hbaseBulkThread;
//    private final ArrayBlockingQueue<Put> hbaseDataQueue;

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
