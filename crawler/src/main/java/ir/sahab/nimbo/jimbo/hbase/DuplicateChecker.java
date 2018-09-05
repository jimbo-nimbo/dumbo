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
    private static Cache<String, HBaseMarkModel> cache = Caffeine.newBuilder()
            .expireAfterWrite(100L, TimeUnit.SECONDS)
            .maximumSize(HBASE_BULK_CAPACITY).build();
    private static ArrayBlockingQueue<HBaseMarkModel> arrayBlockingQueue = new ArrayBlockingQueue<>(HBASE_BULK_CAPACITY);
    private DuplicateChecker(){

    }

    public HBaseMarkModel getShouldFetchMarkModel(String sourceUrl) {
        Timer.Context dcGetShouldFetchRequestsTimeContext = Metrics.getInstance().dcGetShouldFetchRequestsTime();
        HBaseMarkModel hBaseMarkModel = cache.getIfPresent(sourceUrl);
        if (hBaseMarkModel == null) {
            Timer.Context hbaseExistRequestsTimeContext = Metrics.getInstance().hbaseExistRequestsTime();
            hBaseMarkModel = HBase.getInstance().getMark(sourceUrl);
            hbaseExistRequestsTimeContext.stop();
        }
        dcGetShouldFetchRequestsTimeContext.stop();
        return hBaseMarkModel;

    }

    public void add(HBaseMarkModel hBaseMarkModel){
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

    public void updateLastSeen(HBaseMarkModel markModel, String newHash){
        Timer.Context dcUpdateKastSeenRequestsTimeContext = Metrics.getInstance().dcUpdateKastSeenRequestsTime();
        if(markModel.getBodyHash().equals(newHash) && markModel.getDuration() < HBASE_DURATION_MAX) {
            markModel.setDuration(markModel.getDuration() * 2);
        }
        else {
            if(markModel.getDuration() > HBASE_DURATION_MIN) {
                markModel.setDuration(markModel.getDuration() / 2);
            }
        }
        add(markModel);
        dcUpdateKastSeenRequestsTimeContext.close();
    }
    public HBaseMarkModel take(){
        try {
            Metrics.getInstance().markdcTake();
            return arrayBlockingQueue.take();
        } catch (InterruptedException e) {
            logger.error("duplicate Checker cant take and interupted");
        }
        return null;
    }

    public static DuplicateChecker getInstance()
    {
        return duplicateChecker;
    }

    public static Cache<String, HBaseMarkModel> getCache() {
        return cache;
    }

    public static ArrayBlockingQueue<HBaseMarkModel> getArrayBlockingQueue() {
        return arrayBlockingQueue;
    }
}
