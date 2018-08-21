package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static ir.sahab.nimbo.jimbo.main.Config.HBASE_BULK_LIMIT;

public class HBaseBulkHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HBaseBulkHandler.class);

    private final ArrayBlockingQueue<HBaseDataModel> bulkQueue;
    private final List<Put> puts = new ArrayList<>();

    public HBaseBulkHandler(ArrayBlockingQueue<HBaseDataModel> bulkQueue) {
        this.bulkQueue = bulkQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                while (puts.size() < HBASE_BULK_LIMIT) {
                    HBaseDataModel data = bulkQueue.take();
                    if (data.getLinks().size() > 0) {
                        puts.add(HBase.getInstance().getPutData(data));
                    }
                }
                HBase.getInstance().getTable().put(puts);
                puts.clear();
            } catch (InterruptedException | IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
