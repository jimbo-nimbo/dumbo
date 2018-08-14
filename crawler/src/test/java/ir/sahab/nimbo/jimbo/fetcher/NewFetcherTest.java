package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NewFetcherTest {

    private final int queueSize = 10000;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue = new ArrayBlockingQueue<>(queueSize);
    private final ArrayBlockingQueue<WebPageModel> webPagesQueue = new ArrayBlockingQueue<>(queueSize);

    private final int threadCount = 20;
    private final FetcherSetting fetcherSetting = new FetcherSetting(threadCount);



    @Test
    public void testFetch() throws InterruptedException, ExecutionException {

//        NewFetcher newFetcher = new NewFetcher(shuffledLinksQueue, webPagesQueue, fetcherSetting);
//        List<Future<HttpResponse>> futures = new ArrayList<>();
//
//        for (int i = 0; i < 2000; i++) {
//            HttpGet get = new HttpGet("https://en.wikipedia.org/wiki/" + i);
//            futures.add(newFetcher.getClient(0).execute(get, null));
//        }
//
//        for (int i = 0; i < futures.size(); i++) {
//            Future<HttpResponse> responseFuture = futures.get(i);
//
//            responseFuture.get();
//
//            System.out.println("document - number " + i);
//        }

    }

    @Test
    public void testClient3() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException, KeyManagementException, KeyStoreException {
//        List<Future<HttpResponse>> futures = new ArrayList<>();
//        NewFetcher newFetcher = new NewFetcher(shuffledLinksQueue, webPagesQueue, fetcherSetting);
//        CloseableHttpAsyncClient client = newFetcher.getClient(0);
//
//        for (int i = 0; i < 200; i++) {
//            futures.add(client.execute(new HttpGet("https://en.wikipedia.org/wiki/" + i), null));
//        }
//
//        for (int i = 0; i < 200; i++) {
//            HttpGet get = new HttpGet("https://en.wikipedia.org/wiki/" + i);
//            newFetcher.getClient(0).execute(get, null);
//        }
//
//        for (int i = 0; i < futures.size(); i++) {
//            Future<HttpResponse> responseFuture = futures.get(i);
//
//            long time = System.currentTimeMillis();
//            EntityUtils.toString(responseFuture.get().getEntity());
//            System.out.println( i + " ----> " + (System.currentTimeMillis() - time));
//        }
//
//        client.close();
    }

    @Test
    public void testFetchMultiClient() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException, KeyManagementException, KeyStoreException {

//        final int clientsCount = threadCount;
//        final List<Future<HttpResponse>> futures = new ArrayList<>();
//        final NewFetcher newFetcher = new NewFetcher(shuffledLinksQueue, webPagesQueue, fetcherSetting);
//
//        final CloseableHttpAsyncClient clients[] = new CloseableHttpAsyncClient[clientsCount];
//        for (int i = 0; i < clients.length; i++) {
//            clients[i] = newFetcher.getClient(i);
//        }
//
//        for (int i = 0; i < 2000; i++) {
//            CloseableHttpAsyncClient client = clients[i%clientsCount];
//
//            HttpGet request = new HttpGet("https://en.wikipedia.org/wiki/" + i);
//            Future<HttpResponse> future = client.execute(request, null);
//            futures.add(future);
//        }
//
//        for (int i = 0; i < futures.size(); i++) {
//            Future<HttpResponse> responseFuture = futures.get(i);
//
//            EntityUtils.toString(responseFuture.get().getEntity());
//            responseFuture.get();
//
//            System.out.println("document number " + i);
//        }
//
//        for (CloseableHttpAsyncClient client: clients) {
//            client.close();
//        }
    }

    @Test
    public void createClient() throws IOException {
//        NewFetcher newFetcher = new NewFetcher(shuffledLinksQueue, webPagesQueue, fetcherSetting);
//        CloseableHttpAsyncClient client = newFetcher.getClient(0);
//
//        client.close();
    }

    @Test
    public void fetch() throws InterruptedException {
//        NewFetcher newFetcher = new NewFetcher(shuffledLinksQueue, webPagesQueue, fetcherSetting);
//
//        List<Future<HttpResponse>> futures = new ArrayList<>();
//        for (int i = 0; i < 200; i++) {
//            HttpGet get = new HttpGet("https://en.wikipedia.org/wiki/" + i);
//            newFetcher.getClient(0).execute(get, null);
//        }
//
//        Runnable runnable = () -> {
//            for (int i = 0; i < futures.size(); i++) {
//
//                final long time = System.currentTimeMillis();
//                try {
//                    futures.get(i).get();
//                } catch (InterruptedException | ExecutionException e) {
//                    e.printStackTrace();
//                }
//                System.out.println(i + " -> " + (System.currentTimeMillis() - time));
//            }
//        };
//
//        new Thread(runnable).start();
//        Thread.sleep(200000);
    }

    @Test
    public void testWorkers() throws InterruptedException {
//        NewFetcher newFetcher = new NewFetcher(shuffledLinksQueue, webPagesQueue, fetcherSetting);
//
//        int numberOfRecords = 1005;
//        List<String> list = new ArrayList<>();
//        for (int i = 0; i < numberOfRecords; i++) {
//            list.add("https://en.wikipedia.org/wiki/" + i);
//            if (i % 100 == 99) {
//                shuffledLinksQueue.put(list);
//                list.clear();
//            }
//        }
//
//        newFetcher.runWorkers();
//
//        while(true) {
//            System.out.println(webPagesQueue.size());
//            if (webPagesQueue.size() == numberOfRecords) {
//                newFetcher.stop();
//                break;
//            }
//
//            Thread.sleep(1000);
//        }
    }
}