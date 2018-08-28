package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Fetcher {

    // Mostafa: not used?
    private static final Logger logger = LoggerFactory.getLogger(Fetcher.class);

    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    private final Worker[] workers;

    public Fetcher(ArrayBlockingQueue<List<String>> shuffledLinksQueue
            , ArrayBlockingQueue<WebPageModel> rawPagesQueue, FetcherSetting fetcherSetting){
        this.shuffledLinksQueue = shuffledLinksQueue;
        this.rawPagesQueue = rawPagesQueue;

        workers =  new Worker[fetcherSetting.getFetcherThreadCount()];
    }

    public void runWorkers() {
        for (int i = 0; i < workers.length; i++) {
                workers[i] = new Worker(this, i);
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


