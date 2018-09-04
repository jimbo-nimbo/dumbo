package ir.sahab.nimbo.jimbo.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class HBaseBulkMarkHandler{

    private static final Logger logger = LoggerFactory.getLogger(HBaseBulkMarkHandler.class);
    MarkWorker[] markWorkers;
    public HBaseBulkMarkHandler() {
        markWorkers = new MarkWorker[HBASE_MARK_NUMBER_OF_THREAD];
    }

    public void runWorkers() {
        for (int i = 0; i < HBASE_MARK_NUMBER_OF_THREAD; i++) {
            markWorkers[i] = new MarkWorker();
            markWorkers[i].start();
        }
    }


}
