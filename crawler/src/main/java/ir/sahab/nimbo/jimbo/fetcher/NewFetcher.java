package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NewFetcher {

    public static AtomicInteger fetchedPages = new AtomicInteger(0);
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawPagesQueue;

    private final FetcherSetting fetcherSetting;

    public static AtomicInteger linkpassed = new AtomicInteger(1);
    public static AtomicInteger linkNotPassed = new AtomicInteger(1);

    private CloseableHttpAsyncClient[] clients;

    private final Worker[] workers;

    public NewFetcher(ArrayBlockingQueue<List<String>> shuffledLinksQueue
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
            try {
                workers[i] = new Worker(this, i);
            } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                e.printStackTrace();
            }
            new Thread(workers[i]).start();
        }
    }

    public void stop() {
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    ArrayBlockingQueue<List<String>> getShuffledLinksQueue() {
        return shuffledLinksQueue;
    }

    ArrayBlockingQueue<WebPageModel> getRawPagesQueue() {
        return rawPagesQueue;
    }

}


