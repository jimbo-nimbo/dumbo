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

public class HBaseSearch extends AbstractHBase {

    private static final Logger logger = LoggerFactory.getLogger(HBaseSearch.class);

    private static HBaseSearch hbase = new HBaseSearch();

    private HBaseSearch() {
        super(HBASE_TABLE_NAME);
    }

    public static HBaseSearch getInstance() {
        return hbase;
    }

    public Integer[] getNumberOfReferences(List<String> urls) {
        ArrayList<Get> gets = new ArrayList<>();
        Integer[] ans = new Integer[urls.size()];
        for (int i = 0; i < urls.size(); i++) {
            Get get = new Get(DigestUtils.md5Hex(urls.get(i)).getBytes());
            get.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES_BYTES);
            get.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES);
            gets.add(get);
            ans[i] = 0;
        }
        if (urls.size() > 0) {
            Result[] results;
            try {
                results = table.get(gets);
                for (int i = 0; i < results.length; i++) {
                    Result result = results[i];
                    if (result != null) {
                        byte[] res = result.getValue(HBASE_MARK_CF_NAME_BYTES,
                                HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES_BYTES);
                        byte[] url = result.getValue(HBASE_MARK_CF_NAME_BYTES,
                                HBASE_MARK_Q_NAME_URL_BYTES);
                        if (res != null && url != null) {
                            String urlStr = Bytes.toString(url);
                            for (int j = 0; j < urls.size(); j++) {
                                if (urls.get(j).equals(urlStr)) {
                                    ans[j] = Bytes.toInt(res);
                                }
                            }
                        } else {
                            ans[i] = 0;
                        }
                    } else {
                        ans[i] = 0;
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
