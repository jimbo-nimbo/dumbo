package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.fetcher.FetcherThread;
import ir.sahab.nimbo.jimbo.parser.PageExtractor;
import org.jsoup.nodes.Document;

import java.util.concurrent.ArrayBlockingQueue;

public class Crawler
{
    private static final int FETCHER_NUMBER = 100;
    private static final int PARSER_NUMBER = 100;
    private static final int MAX_DOCUMENT = 10000;

    private ArrayBlockingQueue<Document> queue;

    private Runnable[] fetchers;
    private Runnable[] parsers;

    Crawler()
    {
        queue = new ArrayBlockingQueue<Document>(MAX_DOCUMENT);
        fetchers = new Runnable[FETCHER_NUMBER];
        for (int i = 0; i < FETCHER_NUMBER; i++) {
//            fetchers[i] = new FetcherThread(queue);
        }

        parsers = new Runnable[PARSER_NUMBER];
        for (int i = 0; i < PARSER_NUMBER; i++) {
//            parsers[i] = new PageExtractor(queue);
        }

        for (int i = 0; i < FETCHER_NUMBER; i++)
        {
            fetchers[i].run();
        }

        for (int i = 0; i < PARSER_NUMBER; i++)
        {
            parsers[i].run();
        }
    }

}
