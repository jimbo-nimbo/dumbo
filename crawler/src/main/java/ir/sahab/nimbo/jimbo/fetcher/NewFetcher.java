package ir.sahab.nimbo.jimbo.fetcher;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;

public class NewFetcher implements Runnable{

    private final ArrayBlockingQueue<String> shuffledLinksQueue;
    private final ArrayBlockingQueue<String> rawPagesQueue;

    private final FetcherSetting fetcherSetting;

    private final ExecutorService executorService;
    private CloseableHttpAsyncClient[] clients;

    private final Worker[] workers;

    public NewFetcher(ArrayBlockingQueue<String> shuffledLinksQueue
            , ArrayBlockingQueue<String> rawPagesQueue, FetcherSetting fetcherSetting){
        this.shuffledLinksQueue = shuffledLinksQueue;
        this.rawPagesQueue = rawPagesQueue;
        this.fetcherSetting = fetcherSetting;

        executorService = Executors.newFixedThreadPool(fetcherSetting.getFetcherThreadCount());

        workers =  new Worker[fetcherSetting.getFetcherThreadCount()];

        clients = new CloseableHttpAsyncClient[fetcherSetting.getFetcherThreadCount()/10 + 1];

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

    Future<HttpResponse> fetch(String link) {

        final HttpGet request = new HttpGet(link);

//        Future<HttpResponse> html = executorService.submit(() -> {
//            final String s = EntityUtils.toString(clients[0].execute(request, null).get().getEntity());
//            rawPagesQueue.put(s);
////            return s;
//            return clients[0].execute(request, null).get();
//        });
        return clients[0].execute(request, null);
    }

    @Override
    public void run() {

//        while(true) {
//            try {
//                String link = shuffledLinksQueue.take();
//                fetch(link);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

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
            System.out.println("Thread " + i + "started!");
            workers[i] = new Worker(this);
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
    private boolean running = true;

    private final CloseableHttpAsyncClient client;
    private final ArrayBlockingQueue<String> shuffledLinksQueue;
    private final ArrayBlockingQueue<String> rawWebPagesQueue;
    private final NewFetcher newFetcher;

    Worker(NewFetcher newFetcher) {
        this.client = newFetcher.getClient(0);
        this.shuffledLinksQueue = newFetcher.getShuffledLinksQueue();
        this.rawWebPagesQueue = newFetcher.getRawPagesQueue();
        this.newFetcher = newFetcher;
    }

    void stop() {
        running = false;
    }

    @Override
    public void run() {

        while(running)
        {
            try {
                final String link = shuffledLinksQueue.take();
                final HttpGet request = new HttpGet(link);

                final long time = System.currentTimeMillis();

                client.execute(request, null).get();
//                final String s = EntityUtils.toString(newFetcher.getClient().execute(request, null).get().getEntity());
//                newFetcher.getRawPagesQueue().put(s);
                System.out.println("---->" + link + " , " + (System.currentTimeMillis() - time));
                rawWebPagesQueue.put("hello");

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
