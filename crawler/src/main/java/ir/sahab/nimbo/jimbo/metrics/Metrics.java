package ir.sahab.nimbo.jimbo.metrics;

import com.codahale.metrics.*;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.hbase.DuplicateChecker;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

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
    private Meter dcTake;
    private Meter fetcherMarkWorkerNumberOfBulkPacksSend;
    private Meter fetcherMarkWorkerNewLink;
    private Meter fetcherMarkWorkerUpdateLink;

    private Timer lruExistRequests;
    private Timer lruPutRequests;
    private Timer httpRequests;
    private Timer kafkaProduceRequests;
    private Timer kafkaConsumeRequests;
    private Timer hbaseExistRequests;
    private Timer hbasePutMarkRequests;
    private Timer hbasePutBulkDataRequests;
    private Timer hbasePutBulkMarkRequests;
    private Timer hbasePutDataRequests;
    private Timer urlFetchRequests;
    private Timer parserJobRequests;
    private Timer parserElasticPutDataRequests;
    private Timer parserTakeWebPageRequests;
    private Timer parserJsoupParseRequests;
    private Timer parserExtractLinksRequests;
    private Timer fetcherUpdateSiteHBase;
    private Timer fetcherShouldFetchRequests;
    private Timer fetcherAddMarkRequests;
    private Timer fetcherMarkWorkerJobRequests;
    private Timer fetcherMarkWorkerPutRequests;
    private Timer fetcherMarkWorkerCheckLinkRequests;
    private Timer dcAddRequests;
    private Timer dcUpdateLastSeenRequests;
    private Timer dcGetShouldFetchRequests;

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
        Metrics.getInstance().getMetricRegistry().register("bulk.mark.queue.size",
                (Gauge<Integer>) DuplicateChecker.getArrayBlockingQueue()::size);
        Metrics.getInstance().getMetricRegistry().register("bulk.mark.cache.size",
                (Gauge<Long>) DuplicateChecker.getCache()::estimatedSize);

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
        dcTake = metricRegistry.meter("dc.take");
        fetcherMarkWorkerNumberOfBulkPacksSend = metricRegistry
                .meter("fetcher.mark.worker.number.of.bilk.packs.send");
        fetcherMarkWorkerNewLink = metricRegistry.meter("fetcher.mark.worker.newlink");
        fetcherMarkWorkerUpdateLink = metricRegistry.meter("fetcher.mark.worker.updatelink");

        lruExistRequests = metricRegistry.timer("lru.exist.requests");
        lruPutRequests = metricRegistry.timer("lru.put.requests");
        httpRequests = metricRegistry.timer("http.requests");
        kafkaProduceRequests = metricRegistry.timer("kafka.produce.requests");
        kafkaConsumeRequests = metricRegistry.timer("kafka.consume.requests");
        urlFetchRequests = metricRegistry.timer("url.fetch.requests");
        fetcherUpdateSiteHBase = metricRegistry.timer("fetcher.update.site.hbase.requests");
        fetcherShouldFetchRequests = metricRegistry.timer("fetcher.should.fetch.requests");
        fetcherAddMarkRequests = metricRegistry.timer("fetcher.add.mark.requests");
        hbaseExistRequests = metricRegistry.timer("hbase.exist.requests");
        hbasePutMarkRequests = metricRegistry.timer("hbase.put.mark.requests");
        hbasePutBulkDataRequests = metricRegistry.timer("hbase.put.bulk.data.requests");
        hbasePutBulkMarkRequests = metricRegistry.timer("hbase.put.bulk.mark.requests");
        hbasePutDataRequests = metricRegistry.timer("hbase.put.data.requests");
        parserTakeWebPageRequests = metricRegistry.timer("parser.take.webpage.requests");
        parserJsoupParseRequests = metricRegistry.timer("parser.jsoup.parse.requests");
        parserJobRequests = metricRegistry.timer("parser.job.requests");
        parserExtractLinksRequests = metricRegistry.timer("parser.extract.links.requests");
        parserElasticPutDataRequests = metricRegistry.timer("parser.elastic.put.data.reuests");
        dcAddRequests = metricRegistry.timer("dc.add.requests");
        dcGetShouldFetchRequests = metricRegistry.timer("dc.should.fetch.requests");
        dcUpdateLastSeenRequests = metricRegistry.timer("dc.update.last.seen.requests");
        fetcherMarkWorkerPutRequests = metricRegistry.timer("fetcher.mark.worker.put.requests");
        fetcherMarkWorkerJobRequests = metricRegistry.timer("fetcher.mark.worker.job.requests");
        fetcherMarkWorkerCheckLinkRequests = metricRegistry.timer("fetcher.mark.worker.checklink.requests");

    }

    public void startJmxReport() {
        final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
    }

    public void startCsvReport() {
        final CsvReporter reporter = CsvReporter.forRegistry(metricRegistry)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File(Config.METRICS_DIR));
        reporter.start(10, TimeUnit.SECONDS);
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
    public void markNewLinks() {
        newLinks.mark();
    }
    public void markDuplicatedLinks() {
        duplicatedLinks.mark();
    }
    public void markdcTake(){dcTake.mark();}
    public void markFetcherMarkWorkerNumberOfBulkPacksSend(){fetcherMarkWorkerNumberOfBulkPacksSend.mark();}
    public void markFetcherMarkWorkerNewLink(){fetcherMarkWorkerNewLink.mark();}
    public void markFetcherMarkWorkerUpdateLink(){fetcherMarkWorkerUpdateLink.mark();}

    public Timer.Context urlFetchRequestsTime() {
        return urlFetchRequests.time();
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
    public Timer.Context hbasePutBulkDataRequestsTime() {
        return hbasePutBulkDataRequests.time();
    }
    public Timer.Context hbasePutBulkMarkRequestsTime() {
        return hbasePutBulkMarkRequests.time();
    }
    public Timer.Context hbasePutDataRequestsTime() {
        return hbasePutDataRequests.time();
    }
    public Timer.Context parserElasticPutDataRequestsTime() {
        return parserElasticPutDataRequests.time();
    }
    public Timer.Context parserExtractLinksRequestsTime() {
        return parserExtractLinksRequests.time();
    }
    public Timer.Context parserJobRequestsTime() {
        return parserJobRequests.time();
    }
    public Timer.Context parserJsoupParseRequestsTime() {
        return parserJsoupParseRequests.time();
    }
    public Timer.Context parserTakeWebPageRequestsTime() {
        return parserTakeWebPageRequests.time();
    }
    public Timer.Context fetcherUpdateSiteHBaseRequestsTime() {
        return fetcherUpdateSiteHBase.time();
    }
    public Timer.Context fetcherShouldFetchRequestsTime(){return fetcherShouldFetchRequests.time();}
    public Timer.Context fetcherMarkWorkerCheckLinkRequestsTime(){return fetcherMarkWorkerCheckLinkRequests.time();}
    public Timer.Context fetcherAddMarkRequestsTime(){return fetcherAddMarkRequests.time();}
    public Timer.Context dcAddRequestsTime(){return dcAddRequests.time();}
    public Timer.Context dcGetShouldFetchRequestsTime(){return dcGetShouldFetchRequests.time();}
    public Timer.Context dcUpdateKastSeenRequestsTime(){return dcUpdateLastSeenRequests.time();}
    public Timer.Context fetcherMarkWorkerPutRequestsTime(){return fetcherMarkWorkerPutRequests.time();}
    public Timer.Context fetcherMarkWorkerJobRequestsTime(){return fetcherMarkWorkerJobRequests.time();}


    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }


    public void mockInit(){

        ArrayBlockingQueue<List<String>> shuffleQueue = new
                ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<WebPageModel> fetchedQueue = new
                ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue = new
                ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<HBaseDataModel> hbaseDataQueue = new
                ArrayBlockingQueue<>(1000);
        initialize(shuffleQueue, fetchedQueue, elasticQueue, hbaseDataQueue);
    }
}
