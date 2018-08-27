package ir.sahab.nimbo.jimbo.metrics;

import com.codahale.metrics.*;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Metrics {
    private static Metrics ourInstance = new Metrics();

    public static Metrics getInstance() {
        return ourInstance;
    }

    private final MetricRegistry metricRegistry;

    private Meter parsedPages;
    private Meter elasticSubmitFailure;
    private Meter elasticSubmitSuccess;
    private Meter shuffledPacks;
    private Meter fetcherReceivedLinks;
    private Meter malformedUrls;
    private Meter lruHit;
    private Meter lruMiss;
    private Meter successfulFetches;
    private Meter invalidDocuments;
    private Meter extractedLinks;
    private Meter newLinks;
    private Meter duplicatedLinks;

    private Timer lruExistRequests;
    private Timer lruPutRequests;
    private Timer httpRequests;
    private Timer kafkaProduceRequests;
    private Timer kafkaConsumeRequests;
    private Timer hbaseExistRequests;
    private Timer hbasePutMarkRequests;
    private Timer hbasePutBulkRequests;

    private Metrics() {
        metricRegistry = new MetricRegistry();
        // TODO: maybe use static queues and do all the initializations here?
    }

    public void initialize(ArrayBlockingQueue<List<String>> shuffleQueue,
                                ArrayBlockingQueue<WebPageModel> fetchedQueue,
                                ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                                ArrayBlockingQueue<HBaseDataModel> hbaseQueue) {

        metricRegistry.register("uptime", (Gauge<Long>) () -> ManagementFactory.getRuntimeMXBean().getUptime());

        metricRegistry.register("shuffle.queue.size", (Gauge<Integer>) shuffleQueue::size);
        metricRegistry.register("fetched.queue.size", (Gauge<Integer>) fetchedQueue::size);
        metricRegistry.register("elastic.queue.size", (Gauge<Integer>) elasticQueue::size);
        metricRegistry.register("hbase.queue.size", (Gauge<Integer>) hbaseQueue::size);

        parsedPages = metricRegistry.meter("parsed.pages");
        elasticSubmitFailure = metricRegistry.meter("elasticsearch.submit.failure");
        elasticSubmitSuccess = metricRegistry.meter("elasticsearch.submit.success");
        shuffledPacks = metricRegistry.meter("shuffled.packs");
        fetcherReceivedLinks = metricRegistry.meter("fetcher.received.links");
        malformedUrls = metricRegistry.meter("malformed.urls");
        lruHit = metricRegistry.meter("lru.hit");
        lruMiss = metricRegistry.meter("lru.miss");
        metricRegistry.register("lru.miss.ratio", new RatioGauge() {
            @Override
            protected Ratio getRatio() {
                long denominator = lruHit.getCount() + lruMiss.getCount();
                return Ratio.of(lruMiss.getCount(), denominator == 0 ? 1 : denominator);
            }
        });
        successfulFetches = metricRegistry.meter("successful.fetches");
        invalidDocuments = metricRegistry.meter("invalid.documents");
        extractedLinks = metricRegistry.meter("extracted.links");
        newLinks = metricRegistry.meter("new.links");
        duplicatedLinks = metricRegistry.meter("duplicated.links");
        metricRegistry.register("new.links.ratio", new RatioGauge() {
            @Override
            protected Ratio getRatio() {
                long denominator = newLinks.getCount() + duplicatedLinks.getCount();
                return Ratio.of(newLinks.getCount(), denominator == 0 ? 1 : denominator);
            }
        });

        lruExistRequests = metricRegistry.timer("lru.exist.requests");
        lruPutRequests = metricRegistry.timer("lru.put.requests");
        httpRequests = metricRegistry.timer("http.requests");
        kafkaProduceRequests = metricRegistry.timer("kafka.produce.requests");
        kafkaConsumeRequests = metricRegistry.timer("kafka.consume.requests");
        hbaseExistRequests = metricRegistry.timer("hbase.exist.requests");
        hbasePutMarkRequests = metricRegistry.timer("hbase.put.mark.requests");
        hbasePutBulkRequests = metricRegistry.timer("hbase.put.bulk.requests");
    }

    public void startJmxReport() {
        final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
    }

    public void markParsedPages() {
        parsedPages.mark();
    }

    public void markElasticSubmitFailure() {
        elasticSubmitFailure.mark();
    }

    public void markElasticSubmitSuccess() {
        elasticSubmitSuccess.mark();
    }

    public void markShuffledPacks() {
        shuffledPacks.mark();
    }

    public void markFetcherReceivedLinks(long n) {
        fetcherReceivedLinks.mark(n);
    }

    public void markMalformedUrls() {
        malformedUrls.mark();
    }

    public void markLruHit() {
        lruHit.mark();
    }

    public void markLruMiss() {
        lruMiss.mark();
    }

    public void markSuccessfulFetches() {
        successfulFetches.mark();
    }

    public void markInvalidDocuments() {
        invalidDocuments.mark();
    }

    public void markExtractedLinks(long n) {
        extractedLinks.mark(n);
    }

    public Timer.Context lruExistRequestsTime() {
        return lruExistRequests.time();
    }

    public Timer.Context lruPutRequestsTime() {
        return lruPutRequests.time();
    }

    public Timer.Context httpRequestsTime() {
        return httpRequests.time();
    }

    public Timer.Context kafkaProduceRequestsTime() {
        return kafkaProduceRequests.time();
    }

    public Timer.Context kafkaConsumeRequestsTime() {
        return kafkaConsumeRequests.time();
    }

    public Timer.Context hbaseExistRequestsTime() {
        return hbaseExistRequests.time();
    }

    public Timer.Context hbasePutMarkRequestsTime() {
        return hbasePutMarkRequests.time();
    }

    public Timer.Context hbasePutBulkRequestsTime() {
        return hbasePutBulkRequests.time();
    }

    public void markNewLinks() {
        newLinks.mark();
    }

    public void markDuplicatedLinks() {
        duplicatedLinks.mark();
    }
}
