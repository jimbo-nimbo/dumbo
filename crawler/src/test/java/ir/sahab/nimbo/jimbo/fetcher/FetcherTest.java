package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.asynchttpclient.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.asynchttpclient.Dsl.*;

public class FetcherTest {

    private final int queueSize = 10000;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue = new ArrayBlockingQueue<>(queueSize);
    private final ArrayBlockingQueue<WebPageModel> webPagesQueue = new ArrayBlockingQueue<>(queueSize);

    private final int threadCount = 20;
    private final FetcherSetting fetcherSetting = new FetcherSetting(threadCount);

    @Test
    public void testFetch() throws ExecutionException, InterruptedException {
//        AsyncHttpClient client = Dsl.asyncHttpClient(Dsl.config()
//                .setFollowRedirect(true)
//                .setConnectTimeout(fetcherSetting.getTimeout()));
//
//        List<CompletableFuture<Response>> listenableFutures = new ArrayList<>();
//        for (int i = 0; i < 200; i++) {
//            Request request = Dsl.get("https://en.wikipedia.org/wiki/" + i).build();
//            client.
//            ListenableFuture<Response> future = client.executeRequest(request);
//            CompletableFuture<Response> completableFuture = future.toCompletableFuture();
//            listenableFutures.add(completableFuture);
//        }
//
//        for (CompletableFuture<Response> listenableFuture : listenableFutures) {
//            System.out.println(listenableFuture.get().getResponseBody().substring(0, 100));
//        }
    }

}