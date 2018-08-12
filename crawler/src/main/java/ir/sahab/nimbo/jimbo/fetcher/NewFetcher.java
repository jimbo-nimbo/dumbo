package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.hbase.HBase;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class NewFetcher implements Runnable{

    private final ArrayBlockingQueue<String> shuffledLinksQueue;
    private final ArrayBlockingQueue<String> rawPagesQueue;

    private final FetcherSetting fetcherSetting;

    private static final LruCache lruCache = LruCache.getInstance();

    private CloseableHttpAsyncClient[] clients;

    private final Worker[] workers;

    public NewFetcher(ArrayBlockingQueue<String> shuffledLinksQueue
            , ArrayBlockingQueue<String> rawPagesQueue, FetcherSetting fetcherSetting){
        this.shuffledLinksQueue = shuffledLinksQueue;
        this.rawPagesQueue = rawPagesQueue;
        this.fetcherSetting = fetcherSetting;

        workers =  new Worker[fetcherSetting.getFetcherThreadCount()];

        clients = new CloseableHttpAsyncClient[fetcherSetting.getFetcherThreadCount()];

        try {
            for (int i = 0; i < clients.length; i++) {
                clients[i] = createNewClient();
            }
        } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
            //Todo: handle exception
            e.printStackTrace();
        }
    }

    private CloseableHttpAsyncClient createNewClient() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null,
                (certificate, authType) -> true).build();

        CloseableHttpAsyncClient client = HttpAsyncClients.custom().setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
        client.start();

        return client;
    }

    public static boolean checkLink(String link) throws MalformedURLException {
        String host = new URL(link).getHost();
        if (lruCache.exist(host)) {
            //todo: produce to kafka
            return false;
        }

        lruCache.add(host);
        if (HBase.getInstance().existMark(link)){
            lruCache.remove(host);
            return false;
        }

        HBase.getInstance().putMark(link, "1");
        return true;
    }

    @Override
    public void run() {
        runWorkers();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        for (CloseableHttpAsyncClient client : clients) {
            client.close();
        }
    }

    private void runWorkers() {
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(this, i);
            new Thread(workers[i]).start();
        }
    }

    public void stop() {
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    ArrayBlockingQueue<String> getShuffledLinksQueue() {
        return shuffledLinksQueue;
    }

    ArrayBlockingQueue<String> getRawPagesQueue() {
        return rawPagesQueue;
    }

    CloseableHttpAsyncClient getClient(int i) {
        return clients[i];
    }
}

class Worker implements Runnable
{
    private static final int LINKS_PER_CYCLE = 100;

    private boolean running = true;

    private final CloseableHttpAsyncClient client;
    private final ArrayBlockingQueue<String> shuffledLinksQueue;
    private final ArrayBlockingQueue<String> rawWebPagesQueue;
    private final NewFetcher newFetcher;
    private final int workerId;

    Worker(NewFetcher newFetcher, int workerId) {
        this.client = newFetcher.getClient(workerId);
        this.shuffledLinksQueue = newFetcher.getShuffledLinksQueue();
        this.rawWebPagesQueue = newFetcher.getRawPagesQueue();
        this.newFetcher = newFetcher;
        this.workerId = workerId;
    }

    void stop() {
        running = false;
    }

    @Override
    public void run() {

        System.out.println("thread " + workerId + " started!");
        List<Future<HttpResponse>> futures = new ArrayList<>();
        while(running)
        {
            while (futures.size() < LINKS_PER_CYCLE) {
                try {
                    final String link = shuffledLinksQueue.poll(5, TimeUnit.SECONDS);
                    if (link == null) {
                        break;
                    }
                    if (NewFetcher.checkLink(link)){
                        continue;
                    }

                    final HttpGet request = new HttpGet(link);
                    futures.add(client.execute(request, null));
                } catch (InterruptedException | MalformedURLException e) {
                    e.printStackTrace();
                }
            }

            try {

                for (Future<HttpResponse> future : futures) {
                    final HttpResponse response = future.get();
                    final String text = EntityUtils.toString(response.getEntity());
                    rawWebPagesQueue.put(text);
                }

            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }

            futures.clear();
        }
    }
}
