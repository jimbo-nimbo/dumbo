package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.fetcher.FetcherFactory;
import ir.sahab.nimbo.jimbo.parser.PageExtractor;
import ir.sahab.nimbo.jimbo.parser.PageExtractorFactory;
import org.jsoup.nodes.Document;

import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {
    private static final int FETCHER_NUMBER = 300;
    private static final int PARSER_NUMBER = 100;
    private static final int MAX_DOCUMENT = 10000;

    private ArrayBlockingQueue<Document> queue;

    private FetcherFactory fetcherFactory;
    private PageExtractorFactory pageExtractorFactory;

    private Runnable[] fetchers;
    private Runnable[] parsers;

    Crawler() {

        queue = new ArrayBlockingQueue<>(MAX_DOCUMENT);
        fetcherFactory = new FetcherFactory(queue);
        pageExtractorFactory = new PageExtractorFactory(queue);

        fetchers = new Runnable[FETCHER_NUMBER];
        for (int i = 0; i < FETCHER_NUMBER; i++) {
            fetchers[i] = fetcherFactory.newFetcher();
        }

        parsers = new Runnable[PARSER_NUMBER];
        for (int i = 0; i < PARSER_NUMBER; i++) {
            parsers[i] = pageExtractorFactory.getPageExtractor();
        }

        for (int i = 0; i < FETCHER_NUMBER; i++) {
            new Thread(fetchers[i]).start();
        }

        for (int i = 0; i < PARSER_NUMBER; i++) {
            new Thread(parsers[i]).start();
        }

        while (true) {
            synchronized (PageExtractor.tmp) {
                if (System.currentTimeMillis() - PageExtractor.tmp > 1000) {
                    PageExtractor.tmp = System.currentTimeMillis();
                    System.out.println(PageExtractor.t);
                    PageExtractor.t = 0;
                }
            }
        }
    }

}
