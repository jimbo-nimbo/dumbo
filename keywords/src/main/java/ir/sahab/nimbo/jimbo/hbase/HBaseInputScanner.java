package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.Config;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseInputScanner extends AbstractHBase {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHBase.class);

    private static HBaseInputScanner ourInstance = new HBaseInputScanner();

    private ResultScanner resultScanner = null;
    private boolean nullNext = true;

    private HBaseInputScanner() {
        super(Config.HBASE_INPUT_TABLE);
    }

    public static HBaseInputScanner getInstance() {
        return ourInstance;
    }

    @Override
    protected void initializeTable(Connection connection) throws IOException {
    }

    public void initializeScan() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Config.META_CF_NAME.getBytes(), Config.URL_COL_NAME.getBytes());
        resultScanner = table.getScanner(scan);
        nullNext = false;
    }

    public List<String> nextBulk() throws IOException {
        List<String> urls = new ArrayList<>();
        Result result;
        for (int count = 0; count < Config.BULK_SIZE; count++) {
            result = resultScanner.next();
            if (result == null) {
                nullNext = true;
                break;
            }
            urls.add(Bytes.toString(result.getValue(Config.META_CF_NAME.getBytes(), Config.URL_COL_NAME.getBytes())));
        }
        return urls;
    }

    public boolean hasNext() {
        return !nullNext;
    }
}