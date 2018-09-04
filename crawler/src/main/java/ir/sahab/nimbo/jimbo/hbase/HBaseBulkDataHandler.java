package ir.sahab.nimbo.jimbo.hbase;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class HBaseBulkDataHandler{

    private static final Logger logger = LoggerFactory.getLogger(HBaseBulkDataHandler.class);
    DataWorker[] dataWorkers;
    private final ArrayBlockingQueue<HBaseDataModel> bulkQueue;
    public HBaseBulkDataHandler(ArrayBlockingQueue<HBaseDataModel> bulkQueue) {
        this.bulkQueue = bulkQueue;
        dataWorkers = new DataWorker[HBASE_DATA_NUMBER_OF_THREAD];
    }

    public void runWorkers(){

        for(int i = 0; i < HBASE_DATA_NUMBER_OF_THREAD; i++){
            dataWorkers[i] = new DataWorker(bulkQueue);
            dataWorkers[i].start();
        }
    }

}
