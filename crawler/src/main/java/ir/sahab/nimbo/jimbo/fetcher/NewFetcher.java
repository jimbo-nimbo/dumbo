package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.parser.WebPageModel;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NewFetcher {

    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    public static AtomicInteger linkpassed = new AtomicInteger(1);
    public static AtomicInteger linkNotPassed = new AtomicInteger(1);

    private final Worker[] workers;

    public NewFetcher(ArrayBlockingQueue<List<String>> shuffledLinksQueue
            , ArrayBlockingQueue<WebPageModel> rawPagesQueue, FetcherSetting fetcherSetting){
        this.shuffledLinksQueue = shuffledLinksQueue;
        this.rawPagesQueue = rawPagesQueue;

        workers =  new Worker[fetcherSetting.getFetcherThreadCount()];

    }

    public void runWorkers() {
        for (int i = 0; i < workers.length; i++) {
            try {
                workers[i] = new Worker(this, i);
            } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                e.printStackTrace();
            }
            new Thread(workers[i]).start();
        }
    }

    ArrayBlockingQueue<List<String>> getShuffledLinksQueue() {
        return shuffledLinksQueue;
    }

    ArrayBlockingQueue<WebPageModel> getRawPagesQueue() {
        return rawPagesQueue;
    }

}


