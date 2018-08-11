package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
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
    ArrayList<Row> rowArrayList = new ArrayList<>();
    ArrayList<Put> putArrayList = new ArrayList<>();
    Table table;

    HbaseBulkThread(Table table, ArrayBlockingQueue<Put> arrayBlockingQueue){
        this.arrayBlockingQueue = arrayBlockingQueue;
        this.table = table;
    }



    void runInPutMode(){
        if(putArrayList.size() < HBASE_BULK_LIMIT){
            try {
                putArrayList.add(arrayBlockingQueue.take());
            } catch (InterruptedException e) {
                Logger.getInstance().debugLog(e.getMessage());
            }
        }else {
            try {
                table.put(putArrayList);
                putArrayList.clear();
            } catch (IOException e) {
                Logger.getInstance().debugLog(e.getMessage());
            }
        }
    }

    void runInBatchMode(){
        if(rowArrayList.size() < HBASE_BULK_LIMIT){
            try {
                rowArrayList.add(new RowMutations(arrayBlockingQueue.take().getRow()));
            } catch (InterruptedException e) {
                Logger.getInstance().debugLog(e.getMessage());
            }
        }else {
            try {
                table.batch(rowArrayList, new Object[rowArrayList.size()]);
                rowArrayList.clear();
            } catch (IOException e) {
                Logger.getInstance().debugLog(e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        while (isRun){
            runInPutMode();
        }
    }
}
