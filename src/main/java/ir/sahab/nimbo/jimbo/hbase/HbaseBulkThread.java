package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import static ir.sahab.nimbo.jimbo.main.Config.HBASE_BULK_LIMIT;

/**
 * why? to controll the rate of reading fom BlockQueue and put to hbase and also seperate read/write from hbase
 * task from other task
 */
public class HbaseBulkThread implements Runnable {

    boolean isRun = true;
    ArrayBlockingQueue<Put> arrayBlockingQueue;
    ArrayList<Put> arrayList = new ArrayList<>();
    Table table;

    HbaseBulkThread(Table table, ArrayBlockingQueue<Put> arrayBlockingQueue){
        this.arrayBlockingQueue = arrayBlockingQueue;
        this.table = table;
    }

    @Override
    public void run() {
        while (isRun){
            if(arrayList.size() < HBASE_BULK_LIMIT){
                try {
                    arrayList.add(arrayBlockingQueue.take());
                } catch (InterruptedException e) {
                    Logger.getInstance().debugLog(e.getMessage());
                }
            }else {
                try {
                    table.put(arrayList);
                    arrayList.clear();
                } catch (IOException e) {
                    Logger.getInstance().debugLog(e.getMessage());
                }
            }
        }
    }
}
