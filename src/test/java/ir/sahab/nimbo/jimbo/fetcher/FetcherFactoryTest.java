package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.*;

public class FetcherFactoryTest
{
    private FetcherFactory  fetcherFactory;

    @Test
    @Before
    public void getInstance()
    {
        fetcherFactory = new FetcherFactory(new ArrayBlockingQueue(100));
    }

    @Test
    public void testGetFetcher()
    {
        fetcherFactory.newFetcher();
    }
}