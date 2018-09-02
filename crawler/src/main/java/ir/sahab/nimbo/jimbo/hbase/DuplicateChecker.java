package ir.sahab.nimbo.jimbo.hbase;

import com.codahale.metrics.Gauge;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class DuplicateChecker {
    private static DuplicateChecker duplicateChecker = new DuplicateChecker();
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DuplicateChecker.class);
    private Cache<String, HBaseMarkModel> cache;
    private ArrayBlockingQueue<HBaseMarkModel> arrayBlockingQueue;
    private DuplicateChecker(){
        Metrics.getInstance().getMetricRegistry().register("bulk.mark.queue.size",
                (Gauge<Integer>) arrayBlockingQueue::size);
        cache = Caffeine.newBuilder().maximumSize(HBASE_BULK_CAPACITY).build();
        arrayBlockingQueue = new ArrayBlockingQueue<>(HBASE_BULK_CAPACITY);
    }

    public HBaseMarkModel getShouldFetchMarkModel(String sourceUrl){
        HBaseMarkModel hBaseMarkModel = cache.getIfPresent(sourceUrl);
        if(hBaseMarkModel != null){
            return hBaseMarkModel;
        } else {
            return HBase.getInstance().getMark(sourceUrl);
        }
    }

    public void add(HBaseMarkModel hBaseMarkModel){
        cache.put(hBaseMarkModel.getUrl(), hBaseMarkModel);
        try {
            arrayBlockingQueue.put(hBaseMarkModel);
        } catch (InterruptedException e) {
            logger.error("cant add to BQ in duplicateChecker");
        }
    }

    public void updateLastSeen(HBaseMarkModel markModel, String newHash){
        if(markModel.getBodyHash().equals(newHash) && markModel.getDuration() < HBASE_DURATION_MAX) {
            markModel.setDuration(markModel.getDuration() * 2);
        }
        else {
            if(markModel.getDuration() > HBASE_DURATION_MIN) {
                markModel.setDuration(markModel.getDuration() / 2);
            }
        }
        add(markModel);
    }
    public HBaseMarkModel take(){
        try {
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
}
