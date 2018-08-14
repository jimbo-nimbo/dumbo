package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.apache.http.HttpEntity;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Worker implements Runnable {

    private static final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private boolean running = true;
    private static final LruCache lruCache = LruCache.getInstance();

    private final CloseableHttpAsyncClient client;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawWebPagesQueue;
    private final NewFetcher newFetcher;
    private final int workerId;

    Worker(NewFetcher newFetcher, int workerId) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        this.shuffledLinksQueue = newFetcher.getShuffledLinksQueue();
        this.rawWebPagesQueue = newFetcher.getRawPagesQueue();
        this.newFetcher = newFetcher;
        this.workerId = workerId;

        client = createNewClient();
    }

    @Override
    public void run() {
        while(running)
        {
            List<Future<HttpResponse>> futures = new ArrayList<>();
            List<String> urls = new ArrayList<>();
            int i = 0;
            try {
                final List<String> shuffledLinks = shuffledLinksQueue.take();
                for (int i1 = 0; i1 < shuffledLinks.size(); i1++) {
                    String link = shuffledLinks.get(i1);
                    if (checkLink(link)) {
//                        System.out.println(i1 + "<---" + link);
                        futures.add(client.execute(new HttpGet(link), null));
                        urls.add(link);
                    }
                }
                for (; i < futures.size(); i++) {
                    HttpEntity entity = futures.get(i).get().getEntity();
                    if (entity != null) {
                        final String text = EntityUtils.toString(entity);
                        rawWebPagesQueue.put(new WebPageModel(text, urls.get(i)));
                    }
                }

            } catch (InterruptedException e) {
                System.out.println("interupt exeption" + urls.get(i));
            } catch (ExecutionException e) {
//                System.out.println("execution exeption" + urls.get(i));
                //todo: handle
            } catch (IOException e) {
                System.out.println("ioException exeption" + urls.get(i));
            }
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

    private CloseableHttpAsyncClient createNewClient() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null,
                (certificate, authType) -> true).build();

        CloseableHttpAsyncClient client = HttpAsyncClients.custom().setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
        client.start();

        return client;
    }

}
