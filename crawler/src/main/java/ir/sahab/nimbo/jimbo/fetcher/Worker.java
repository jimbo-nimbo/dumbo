package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.hbase.HBase;
import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class Worker implements Runnable {

    public static final AtomicInteger TOOK_LINKS = new AtomicInteger(0);
    private static int tookLinks;
    public static final AtomicInteger SHUFFLED_PACK_COUNT = new AtomicInteger(0);
    private static int shuffledPackCount;

    public static final AtomicInteger BAD_LINKS = new AtomicInteger(0);
    private static int badLinks;

    public static final AtomicLong LRU_GET_REQUEST_TIME =new AtomicLong(0);
    private static long lruGetRequestTime;
    public static final AtomicInteger LRU_HIT = new AtomicInteger(1);
    private static int lruHit;
    public static final AtomicInteger LRU_MISS = new AtomicInteger(1);
    private static int lruMiss;

    public static final AtomicLong LRU_ADD_REQUEST_TIME = new AtomicLong(0L);
    private static long lruAddRequestTime;

    public static final AtomicLong PRODUCE_KAFKA_TIME = new AtomicLong(0L);
    private static long produceKafkaTime;

    public static final AtomicInteger FETCHED_LINKS = new AtomicInteger(1);
    private static int fetchedLinks;
    public static final AtomicLong FETCHING_TIME = new AtomicLong();
    private static long fetchingTime;

    public static final AtomicLong PUTTING_TIME = new AtomicLong(0L);
    private static long puttingTime;

    private static DecimalFormat df = new DecimalFormat("#.00");

    public static String log() {
        final StringBuilder stringBuilder = new StringBuilder();

        final int newPacks = SHUFFLED_PACK_COUNT.get() - shuffledPackCount;
        final int newLinks = TOOK_LINKS.get() - tookLinks;
        stringBuilder.append("shuffled packs: " + SHUFFLED_PACK_COUNT.get() + "(+" + newPacks + ")");
        stringBuilder.append(", shuffled packs average size: " +
                (newPacks == 0 ? "-" : df.format(newLinks/newPacks)));
        stringBuilder.append(", link received: " + TOOK_LINKS.get() + "(+" + newLinks + ")");
        tookLinks = TOOK_LINKS.get();
        shuffledPackCount = SHUFFLED_PACK_COUNT.get();

        final int newBadLinks = BAD_LINKS.get() - badLinks;
        stringBuilder.append(", bad links: " + BAD_LINKS.get() + "(+" + newBadLinks + ")");
        badLinks = BAD_LINKS.get();

        final int newLruHit = LRU_HIT.get() - lruHit;
        final int newLruMiss = LRU_MISS.get() - lruMiss;
        final double averageTimePerRequest =
                LRU_GET_REQUEST_TIME.doubleValue()/(LRU_HIT.doubleValue() - LRU_MISS.doubleValue());
        final double averageTimeForAddRequest =
                LRU_ADD_REQUEST_TIME.doubleValue()/LRU_MISS.doubleValue();
        stringBuilder.append("\nTotal of " + (LRU_HIT.get() + LRU_MISS.get())
                + "(+" + (newLruHit + newLruMiss) + ") request to lru cache, Miss:"
                + LRU_MISS.get() + "(+" + newLruMiss + "), Hit:" + LRU_HIT.get() + "(+" + newLruHit + ")" +
                ", average time per request: " + df.format(averageTimePerRequest) +
                ", and for add request: " + df.format(averageTimeForAddRequest));
        lruHit = LRU_HIT.get();
        lruMiss = LRU_MISS.get();

        final double kafkaRecordTime = PRODUCE_KAFKA_TIME.doubleValue()/LRU_HIT.doubleValue();
        final int newFetchedLink = FETCHED_LINKS.get() - fetchedLinks;
        final double averageTimePerFetch = FETCHING_TIME.doubleValue()/FETCHED_LINKS.doubleValue();
        final double averageTimePerPut = PUTTING_TIME.doubleValue()/FETCHED_LINKS.doubleValue();
        stringBuilder.append("\nTime to produce a kafka record: " + df.format(kafkaRecordTime)
                + ", fetched links: " + FETCHED_LINKS.get() + "(+" + newFetchedLink +
                "), average fetch time: " + df.format(averageTimePerFetch) +
                "), average put time: " + df.format(averageTimePerPut));

        fetchedLinks = FETCHED_LINKS.get();
        return stringBuilder.toString();
    }

    private static final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private boolean running = true;
    private static final LruCache lruCache = LruCache.getInstance();

    private static final int timout = new FetcherSetting().getTimout();

    private final CloseableHttpAsyncClient client;
    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawWebPagesQueue;
    private final int workerId;

    Worker(Fetcher fetcher, int workerId) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        this.shuffledLinksQueue = fetcher.getShuffledLinksQueue();
        this.rawWebPagesQueue = fetcher.getRawPagesQueue();
        this.workerId = workerId;

        client = createNewClient();
    }

    @Override
    public void run() {
        while(running)
        {
//            List<Future<HttpResponse>> futures = new ArrayList<>();
//            List<String> urls = new ArrayList<>();
            try {
                final List<String> shuffledLinks = shuffledLinksQueue.take();

                TOOK_LINKS.addAndGet(shuffledLinks.size());
                SHUFFLED_PACK_COUNT.incrementAndGet();

                for (String shuffledLink : shuffledLinks) {
                    if (checkLink(shuffledLink)) {

                        try {
                            long tmp = System.currentTimeMillis();
                            String text = Jsoup.connect(shuffledLink).timeout(timout)
                                    .validateTLSCertificates(false).get().html();
                            FETCHING_TIME.addAndGet(System.currentTimeMillis() - tmp);
                            rawWebPagesQueue.add(new WebPageModel(text, shuffledLink));
                            FETCHED_LINKS.incrementAndGet();

                        } catch (IOException e) {
                            //todo
                        }
                    }
                }

//                for (String link : shuffledLinks) {
//                    if (checkLink(link)) {
//                        futures.add(client.execute(new HttpGet(link), null));
//                        urls.add(link);
//
//                    }
//                }
//
//                for (int i= 0; i < futures.size(); i++) {
//
//                    long tmp = System.currentTimeMillis();
//                    HttpEntity entity = futures.get(i).get().getEntity();
//                    FETCHING_TIME.addAndGet(System.currentTimeMillis() - tmp);
//                    FETCHED_LINKS.incrementAndGet();
//
//                    if (entity != null) {
//                        final String text = EntityUtils.toString(entity);
//                        tmp = System.currentTimeMillis();
//                        rawWebPagesQueue.put(new WebPageModel(text, urls.get(i)));
//                        PUTTING_TIME.addAndGet(System.currentTimeMillis() - tmp);
//                    }
//                }

            } catch (InterruptedException exception) {
                //todo
            }
        }
    }

    private boolean checkLink(String link) {

        URL url;
        try {
            url = new URL(link);
            url.toURI();
        } catch (URISyntaxException | MalformedURLException e) {
            BAD_LINKS.incrementAndGet();
            return false;
        }

        String host = url.getHost();

        long tmp = System.currentTimeMillis();
        boolean exist = lruCache.exist(host);
        LRU_GET_REQUEST_TIME.addAndGet(System.currentTimeMillis() - tmp);

        if (exist) {
            LRU_HIT.incrementAndGet();

            tmp = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null, link));
            PRODUCE_KAFKA_TIME.addAndGet(System.currentTimeMillis() - tmp);

            return false;
        }
        LRU_MISS.incrementAndGet();

        tmp = System.currentTimeMillis();
        lruCache.add(host);
        LRU_ADD_REQUEST_TIME.addAndGet(System.currentTimeMillis() - tmp);


        if (HBase.getInstance().existMark(link)){
            lruCache.remove(host);
            return false;
        }

        HBase.getInstance().putMark(link, "1");

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
