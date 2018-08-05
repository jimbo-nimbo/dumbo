package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;

public class FetcherTest {

    @Test
    public void getUrlBody() {
        FetcherFactory fetcherFactory = new FetcherFactory(new ArrayBlockingQueue(50));
        fetcherFactory.newFetcher().run();
    }

    @Test
    public void run() {
    }
}