package ir.sahab.nimbo.jimbo.hbase;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static ir.sahab.nimbo.jimbo.main.Config.HBASE_MARK_BULK_LIMIT;

public class MarkWorker extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MarkWorker.class);

    @Override
    public void run() {
        Timer.Context fetcherMarkWorkerJobRequestsTimeContext = Metrics.getInstance().fetcherMarkWorkerJobRequestsTime();
        ArrayList<Put> puts = new ArrayList<>();
        while (true) {
            for (int i = 0; i < HBASE_MARK_BULK_LIMIT; i++) {
                puts.add(HBase.getInstance().getPutMark(DuplicateChecker.getInstance().take()));
            }
            try {
                Metrics.getInstance().markFetcherMarkWorkerNumberOfBulkPacksSend();
                Timer.Context fetcherMarkWorkerPutRequestsTimeContext = Metrics.getInstance().fetcherMarkWorkerPutRequestsTime();
                HBase.getInstance().getTable().put(puts);
                fetcherMarkWorkerPutRequestsTimeContext.stop();
            } catch (IOException e) {
                logger.error("error in fetcher Mark worker");
            }
            puts.clear();
            fetcherMarkWorkerJobRequestsTimeContext.stop();
        }
    }
}
