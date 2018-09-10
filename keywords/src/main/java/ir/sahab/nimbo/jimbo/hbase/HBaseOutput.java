package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.Config;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseOutput extends AbstractHBase {
    private static HBaseOutput ourInstance = new HBaseOutput();

    private final List<Put> puts = new ArrayList<>();

    public static HBaseOutput getInstance() {
        return ourInstance;
    }

    private HBaseOutput() {
        super(Config.HBASE_OUTPUT_TABLE);
    }

    @Override
    protected void initializeTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            return;
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor("Data"));
        admin.createTable(desc);
    }

    public void appendPut(String url, List<String> keywords) {
        Put p;
        p = new Put(makeRowKey(url).getBytes());
        p.addColumn("Data".getBytes(), "url".getBytes(), url.getBytes());
        for (int i = 0; i < keywords.size(); i++) {
            String keyword = keywords.get(i);
            p.addColumn("Data".getBytes(), String.valueOf(i).getBytes(), keyword.getBytes());
        }
        puts.add(p);
    }

    public void sendPuts() throws IOException {
        table.put(puts);
    }
}
