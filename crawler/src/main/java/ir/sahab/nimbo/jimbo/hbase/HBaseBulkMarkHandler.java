package ir.sahab.nimbo.jimbo.hbase;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static ir.sahab.nimbo.jimbo.main.Config.HBASE_BULK_LIMIT;
import static ir.sahab.nimbo.jimbo.main.Config.HBASE_NUMBER_OF_THREAD;

public class HBaseBulkMarkHandler{

    private static final Logger logger = LoggerFactory.getLogger(HBaseBulkMarkHandler.class);
    MarkWorker[] markWorkers;
    public HBaseBulkMarkHandler() {
        markWorkers = new MarkWorker[HBASE_NUMBER_OF_THREAD];
    }

    public void runWorkers() {
        for (int i = 0; i < HBASE_NUMBER_OF_THREAD; i++) {
            markWorkers[i] = new MarkWorker();
            markWorkers[i].start();
        }
    }


}
