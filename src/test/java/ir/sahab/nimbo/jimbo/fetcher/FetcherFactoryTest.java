package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;

public class FetcherFactoryTest {
    private FetcherFactory fetcherFactory;

    @Test
    @Before
    public void getInstance() {
        fetcherFactory = new FetcherFactory(new ArrayBlockingQueue(100));
    }

    @Test
    public void testGetFetcher() {
        fetcherFactory.newFetcher();
    }
}