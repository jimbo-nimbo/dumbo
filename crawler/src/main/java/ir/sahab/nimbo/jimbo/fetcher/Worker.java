package ir.sahab.nimbo.jimbo.fetcher;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.hbase.DuplicateChecker;
import ir.sahab.nimbo.jimbo.hbase.HBaseMarkModel;
import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;


public class Worker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private static final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private boolean running = true;
    private static final LruCache lruCache = LruCache.getInstance();

    private static final int timeout = new FetcherSetting().getTimeout();

    private final ArrayBlockingQueue<List<String>> shuffledLinksQueue;
    private final ArrayBlockingQueue<WebPageModel> rawWebPagesQueue;
    private final int workerId;

    Worker(Fetcher fetcher, int workerId) {
        this.shuffledLinksQueue = fetcher.getShuffledLinksQueue();
        this.rawWebPagesQueue = fetcher.getRawPagesQueue();
        this.workerId = workerId;
    }

    @Override
    public void run() {
        while (running) {
            try {
                final List<String> shuffledLinks = shuffledLinksQueue.take();
                Metrics.getInstance().markFetcherReceivedLinks(shuffledLinks.size());
                for (String shuffledLink : shuffledLinks) {
                    Timer.Context urlFetchTimeContext = Metrics.getInstance().urlFetchRequestsTime();
                    //250-300
                    Timer.Context fetcherMarkWorkerCheckLinkRequestsTimeContext =
                            Metrics.getInstance().fetcherMarkWorkerCheckLinkRequestsTime();
                    //1-4
                    HBaseMarkModel markModel = checkLink(shuffledLink);
                    fetcherMarkWorkerCheckLinkRequestsTimeContext.stop();
                    if (markModel != null) {
                        Timer.Context httpTimeContext = Metrics.getInstance().httpRequestsTime();
                        //1060
                        try {
                            Document doc = Jsoup.connect(shuffledLink).timeout(timeout)
                                    .validateTLSCertificates(false).get();
                            String html = doc.html();
                            String text = doc.text();
                            rawWebPagesQueue.put(new WebPageModel(html, shuffledLink));
                            Metrics.getInstance().markSuccessfulFetches();
                            Timer.Context fetcherUpdateSiteHBaseRequestsTimeContext =
                                    Metrics.getInstance().fetcherUpdateSiteHBaseRequestsTime();
                            //1
                            DuplicateChecker.getInstance().updateLastSeen(markModel, DigestUtils.md5Hex(text));
                            fetcherUpdateSiteHBaseRequestsTimeContext.stop();
                        } catch (IOException | IllegalArgumentException |
                                StringIndexOutOfBoundsException | ArrayIndexOutOfBoundsException |
                                OutOfMemoryError e) {
                            logger.error(e.getMessage());
                        } finally {
                            httpTimeContext.stop();
                        }
                    }
                    urlFetchTimeContext.stop();
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private HBaseMarkModel checkLink(String link) {
        URL url;
        try {
            url = new URL(link);
            url.toURI();
        } catch (URISyntaxException | MalformedURLException e) {
            Metrics.getInstance().markMalformedUrls();
            return null;
        }
        String host = url.getHost();
        Timer.Context lruExistTimeContext = Metrics.getInstance().lruExistRequestsTime();
        boolean lruExist = lruCache.exist(host);
        lruExistTimeContext.stop();
        if (lruExist) {
            Metrics.getInstance().markLruHit();
            Timer.Context kafkaProduceTimeContext = Metrics.getInstance().kafkaProduceRequestsTime();
            producer.send(new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null, link));
            kafkaProduceTimeContext.stop();
            return null;
        }

        Metrics.getInstance().markLruMiss();
        Timer.Context lruPutTimeContext = Metrics.getInstance().lruPutRequestsTime();
        lruCache.add(host);
        lruPutTimeContext.stop();
        Timer.Context fetcherShouldFetchRequestsTimeContext = Metrics.getInstance().fetcherShouldFetchRequestsTime();
        HBaseMarkModel shouldFetchMarkModel = DuplicateChecker.getInstance().getShouldFetchMarkModel(link);
        fetcherShouldFetchRequestsTimeContext.stop();
        if (shouldFetchMarkModel != null &&
                shouldFetchMarkModel.getDuration() +
                        shouldFetchMarkModel.getLastSeen() > System.currentTimeMillis()) {
            Metrics.getInstance().markDuplicatedLinks();
            lruCache.remove(host);
            return null;
        }
        if (shouldFetchMarkModel == null) {
            Metrics.getInstance().markFetcherMarkWorkerNewLink();
            shouldFetchMarkModel = new HBaseMarkModel(link, System.currentTimeMillis(),
                    Config.HBASE_MARK_DEFAULT_SEEN_DURATION, "");
        } else {
            Metrics.getInstance().markFetcherMarkWorkerUpdateLink();
        }
        return shouldFetchMarkModel;
    }

}
