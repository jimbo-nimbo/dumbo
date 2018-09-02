package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;

import static ir.sahab.nimbo.jimbo.main.Config.HBASE_BULK_LIMIT;

public class MarkWorker extends Thread {
    @Override
    public void run() {
        ArrayList<Put> puts = new ArrayList<>();
        while (true) {
            for (int i = 0; i < HBASE_BULK_LIMIT; i++) {
                puts.add(HBase.getInstance().getPutMark(DuplicateChecker.getInstance().take()));
            }
            try {
                HBase.getInstance().getTable().put(puts);
            } catch (IOException e) {
                e.printStackTrace();
            }
            puts.clear();
        }
    }
}
