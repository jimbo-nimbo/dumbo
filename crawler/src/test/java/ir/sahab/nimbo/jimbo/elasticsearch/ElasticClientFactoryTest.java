package ir.sahab.nimbo.jimbo.elasticsearch;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ElasticClientFactoryTest {

    CloseableHttpAsyncClient testGoosale()
            throws InterruptedException, ExecutionException, IOException,
            KeyStoreException, NoSuchAlgorithmException, KeyManagementException {


        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null,
                (certificate, authType) -> true).build();
        CloseableHttpAsyncClient client = HttpAsyncClients.custom().setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
        client.start();

        return client;
    }

    Future<HttpResponse> goosale(CloseableHttpAsyncClient client, String uri) {

        HttpGet request = new HttpGet(uri);
        Future<HttpResponse> future = client.execute(request, null);

        return future;
    }

    @Test
    public void testClient() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException, KeyManagementException, KeyStoreException {
        List<Future<HttpResponse>> futures = new ArrayList<>();
        CloseableHttpAsyncClient client = testGoosale();

        for (int i = 0; i < 200; i++) {
            futures.add(goosale(client, "https://en.wikipedia.org/wiki/" + i));
        }

        for (int i = 0; i < futures.size(); i++) {
            Future<HttpResponse> responseFuture = futures.get(i);

            EntityUtils.toString(responseFuture.get().getEntity());

            System.out.println("document number " + i);
        }

        client.close();
    }

    @Test
    public void completeTest()
            throws ElasticCannotLoadException, IOException, InterruptedException,
            ExecutionException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        ArrayBlockingQueue<ElasticsearchWebpageModel> queue = new ArrayBlockingQueue<>(10000);
        ElasticsearchThreadFactory elasticsearchThreadFactory = new ElasticsearchThreadFactory(queue);

        ElasticSearchThread newThread = elasticsearchThreadFactory.createNewThread();


        List<ElasticsearchWebpageModel> models = new ArrayList<>();

        List<Future<HttpResponse>> futures = new ArrayList<>();
        CloseableHttpAsyncClient client = testGoosale();

        for (int i = 0; i < 200; i++) {
            futures.add(goosale(client, "https://en.wikipedia.org/wiki/" + i));
        }

        for (int i = 0; i < futures.size(); i++) {
            Future<HttpResponse> responseFuture = futures.get(i);

            Document document = Jsoup.parse(EntityUtils.toString(responseFuture.get().getEntity()));

            models.add(new ElasticsearchWebpageModel("https://en.wikipedia.org/wiki/" + i,
                    document.text(), document.title(), "description"));

            System.out.println("document number " + i);
        }

        System.out.println("action phase!! >:]");
        Thread.sleep(5000L);

        new Thread(newThread).start();

        for (int i = 0; i < models.size(); i++) {
            ElasticsearchWebpageModel model = models.get(i);
            System.out.println("putting doc number " + i + ", queue size : " + queue.size());
            queue.put(model);
            Thread.sleep(10L);
        }

        Thread.sleep(10000L);

    }
}