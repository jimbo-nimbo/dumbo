package ir.sahab.nimbo.jimbo.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ir.sahab.nimbo.jimbo.elastic.ConfigSearch.*;

public class HBaseKeyWord extends AbstractHBase {

    private static final Logger logger = LoggerFactory.getLogger(HBaseKeyWord.class);

    private static HBaseKeyWord hbase = new HBaseKeyWord();

    private HBaseKeyWord() {
        super("Keyword10Table");
    }

    public static HBaseKeyWord getInstance() {
        return hbase;
    }

    public String[] getKeywords(List<String> urls) {
        ArrayList<Get> gets = new ArrayList<>();
        String[] ans = new String[3 * urls.size()];
        for (int i = 0; i < urls.size(); i++) {
            Get get = new Get(DigestUtils.md5Hex(urls.get(i)).getBytes());
            get.addFamily("Data".getBytes());
//            get.addColumn(HBASE_DATA_CF_NAME_BYTES, Bytes.toBytes("2"));
//            get.addColumn(HBASE_DATA_CF_NAME_BYTES, Bytes.toBytes("3"));
            gets.add(get);
            ans[i] = "";
        }
        if (urls.size() > 0) {
            Result[] results;
            try {
                results = table.get(gets);
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    System.err.println("11");
                    if (result != null) {
                        System.err.println("22");
                        byte[] res1 = result.getValue("Data".getBytes(),
                                String.valueOf(0).getBytes());
                        byte[] res2 = result.getValue("Data".getBytes(),
                                String.valueOf(1).getBytes());
                        byte[] res3 = result.getValue("Data".getBytes(),
                                String.valueOf(2).getBytes());
                        for (int j = 0; j < urls.size(); j++) {
                            if (DigestUtils.md5Hex(urls.get(j)).equals(Bytes.toString(result.getRow()))) {
                                System.err.println("33");
                                if (res1 != null) {
                                    ans[3 * j] = Bytes.toString(res1);
                                } else {
                                    ans[3 * j] = "";
                                }
                                if (res2 != null) {
                                    ans[3 * j + 1] = Bytes.toString(res2);
                                } else {
                                    ans[3 * j + 1] = "";
                                }
                                if (res1 != null) {
                                    ans[3 * j + 2] = Bytes.toString(res3);
                                } else {
                                    ans[3 * j + 2] = "";
                                }
                            } else {
                                ans[3 * i] = ans[3 * i + 1] = ans[3 * i + 2] = "";
                            }
                        }
                    }
                }
                return ans;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return ans;
    }

    @Override
    protected void initializeTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            return;
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(HBASE_DATA_CF_NAME));
        desc.addFamily(new HColumnDescriptor(HBASE_MARK_CF_NAME));
        // TODO: region bandy
        admin.createTable(desc);
    }
}
