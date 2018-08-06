package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.fetcher.FetcherFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.PageExtractor;
import ir.sahab.nimbo.jimbo.parser.PageExtractorFactory;
import org.jsoup.nodes.Document;

import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {

    private static final int FETCHER_NUMBER = Config.FETCHER_THREAD_NUM;
    private static final int PARSER_NUMBER = Config.PARSER_THREAD_NUM;
    private static final int MAX_DOCUMENT = Config.BLOCKING_QUEUE_SIZE;

    private final ArrayBlockingQueue<Document> queue;

    private final Runnable[] fetchers;
    private final Runnable[] parsers;

    public Crawler() {

        queue = new ArrayBlockingQueue<>(MAX_DOCUMENT);
        final FetcherFactory fetcherFactory = new FetcherFactory(queue);
        final PageExtractorFactory pageExtractorFactory = new PageExtractorFactory(queue);

        fetchers = new Runnable[FETCHER_NUMBER];
        for (int i = 0; i < FETCHER_NUMBER; i++) {
            fetchers[i] = fetcherFactory.newFetcher();
        }

        parsers = new Runnable[PARSER_NUMBER];
        for (int i = 0; i < PARSER_NUMBER; i++) {
            parsers[i] = pageExtractorFactory.getPageExtractor();
        }
    }

    /**
     * start threads
     * and log data every 1 second
     */
    public void run()
    {
        for (int i = 0; i < FETCHER_NUMBER; i++) {
            new Thread(fetchers[i]).start();
        }

        for (int i = 0; i < PARSER_NUMBER; i++) {
            new Thread(parsers[i]).start();
        }

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronized (PageExtractor.pageCounter) {
                System.out.println(PageExtractor.pageCounter + " queue size : " + queue.size());
                PageExtractor.pageCounter = 0L;
            }
        }
    }

}
