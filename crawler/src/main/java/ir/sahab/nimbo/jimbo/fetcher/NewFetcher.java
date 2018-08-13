package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NewFetcher {

    private final ArrayBlockingQueue<String> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    private final FetcherSetting fetcherSetting;

    public static AtomicInteger linkpassed = new AtomicInteger(1);
    public static AtomicInteger linkNotPassed = new AtomicInteger(1);

    private CloseableHttpAsyncClient[] clients;

    private final Worker[] workers;

    public NewFetcher(ArrayBlockingQueue<String> shuffledLinksQueue
            , ArrayBlockingQueue<WebPageModel> rawPagesQueue, FetcherSetting fetcherSetting){
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




    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        for (CloseableHttpAsyncClient client : clients) {
            client.close();
        }
    }

    public void runWorkers() {
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

    ArrayBlockingQueue<WebPageModel> getRawPagesQueue() {
        return rawPagesQueue;
    }

    CloseableHttpAsyncClient getClient(int i) {
        return clients[i];
    }
}

class Worker implements Runnable
{
    private static final int LINKS_PER_CYCLE = 100;

    private static final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private boolean running = true;
    private static final LruCache lruCache = LruCache.getInstance();

    private final CloseableHttpAsyncClient client;
    private final ArrayBlockingQueue<String> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawWebPagesQueue;
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

        List<Future<HttpResponse>> futures = new ArrayList<>();
        List<String> links = new ArrayList<>();
        while(running)
        {
            while (futures.size() < LINKS_PER_CYCLE) {
                try {
                    final String link = shuffledLinksQueue.poll(5, TimeUnit.SECONDS);
                    if (link == null && futures.size() != 0) {
                        break;
                    }
                    if (!checkLink(link)){
                        continue;
                    }

                    final HttpGet request = new HttpGet(link);
                    futures.add(client.execute(request, null));
                    links.add(link);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            int i = 0;
            try {

                if (futures.size() != 0)
                    System.out.println(futures.size());
                for (; i < futures.size(); i++) {
                    final HttpResponse response = futures.get(i).get();
                    final String text = EntityUtils.toString(response.getEntity());
                    rawWebPagesQueue.put(new WebPageModel(text, links.get(i)));
                }

            } catch (InterruptedException e) {
                System.out.println("interupt exeption" + links.get(i));
            } catch (ExecutionException e) {
//                System.out.println("execution exeption" + links.get(i));
                //todo: handle
            } catch (IOException e) {
                System.out.println("ioException exeption" + links.get(i));
            }

            futures.clear();
            links.clear();
        }
    }

    static boolean checkLink(String link) {

        URL url;
        try {
            url = new URL(link);
            url.toURI();
        } catch (URISyntaxException | MalformedURLException e) {
            return false;
        }

        String host = url.getHost();
        if (lruCache.exist(host)) {
            producer.send(new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null, link));

            NewFetcher.linkNotPassed.incrementAndGet();
            return false;
        }

        NewFetcher.linkpassed.incrementAndGet();

        lruCache.add(host);
//        if (HBase.getInstance().existMark(link)){
//            lruCache.remove(host);
//            return false;
//        }
//
//        HBase.getInstance().putMark(link, "1");
        return true;
    }
}