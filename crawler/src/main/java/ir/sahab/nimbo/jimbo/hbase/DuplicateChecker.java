package ir.sahab.nimbo.jimbo.hbase;

import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class DuplicateChecker {
    private static DuplicateChecker duplicateChecker = new DuplicateChecker();
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DuplicateChecker.class);
    static Cache<String, HBaseMarkModel> cache = Caffeine.newBuilder()
            .expireAfterWrite(HBASE_COFFEIN_CACHE_TIMEOUT, TimeUnit.SECONDS)
            .maximumSize(HBASE_BULK_CAPACITY).build();
    static ArrayBlockingQueue<HBaseMarkModel> arrayBlockingQueue = new ArrayBlockingQueue<>(HBASE_BULK_CAPACITY);


    private DuplicateChecker() {

    }

    public boolean exist(String url){
        return cache.getIfPresent(url) != null;
    }

    public HBaseMarkModel getShouldFetchMarkModel(String sourceUrl) {
        Metrics.getInstance().markdcShouldFetch();
        Timer.Context dcGetShouldFetchRequestsTimeContext = Metrics.getInstance().dcGetShouldFetchRequestsTime();
        HBaseMarkModel hBaseMarkModel = cache.getIfPresent(sourceUrl);
        if (hBaseMarkModel == null) {
            Timer.Context hbaseExistRequestsTimeContext = Metrics.getInstance().hbaseExistRequestsTime();
            hBaseMarkModel = HBase.getInstance().getMark(sourceUrl);
            hbaseExistRequestsTimeContext.stop();
            Metrics.getInstance().markdcCheckWithHBase();
        } else {
            Metrics.getInstance().markdcCheckWithCache();
        }
        dcGetShouldFetchRequestsTimeContext.stop();
        return hBaseMarkModel;

    }

    public void add(HBaseMarkModel hBaseMarkModel) {
        Metrics.getInstance().markdcAdd();
        Timer.Context dcAddRequestsTimeContext = Metrics.getInstance().dcAddRequestsTime();
        cache.put(hBaseMarkModel.getUrl(), hBaseMarkModel);
        try {
            arrayBlockingQueue.put(hBaseMarkModel);
        } catch (InterruptedException e) {
            logger.error("cant add to BQ in duplicateChecker");
        } finally {
            dcAddRequestsTimeContext.stop();
        }

    }

    public void updateLastSeen(HBaseMarkModel markModel, String newHash) {
        Timer.Context dcUpdateLastSeenRequestsTimeContext = Metrics.getInstance().dcUpdateLastSeenRequestsTime();
        Metrics.getInstance().markdcUpdate();
        if (markModel.getBodyHash().equals(newHash)) {
            if (markModel.getDuration() < HBASE_DURATION_MAX) {
                Metrics.getInstance().markDcPageNotChanged();
                markModel.setDuration(markModel.getDuration() * 2);
                add(markModel);
            }
        } else {
            if (markModel.getDuration() > HBASE_DURATION_MIN) {
                Metrics.getInstance().markDcPageChanged();
                markModel.setDuration(markModel.getDuration() / 2);
                markModel.setBodyHash(newHash);
                add(markModel);
            }
        }
        dcUpdateLastSeenRequestsTimeContext.close();
    }

    public HBaseMarkModel take() {
        try {
            Metrics.getInstance().markdcTake();
            return arrayBlockingQueue.take();
        } catch (InterruptedException e) {
            logger.error("duplicate Checker cant take and interupted");
        }
        return null;
    }

    public static DuplicateChecker getInstance() {
        return duplicateChecker;
    }

    static Cache<String, HBaseMarkModel> getCache() {
        return cache;
    }

    static ArrayBlockingQueue<HBaseMarkModel> getArrayBlockingQueue() {
        return arrayBlockingQueue;
    }

}
