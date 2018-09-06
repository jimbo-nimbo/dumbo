package ir.sahab.nimbo.jimbo.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Objects;

import static ir.sahab.nimbo.jimbo.elastic.Config.*;

public class HBase extends AbstractHBase {

    private static final Logger logger = LoggerFactory.getLogger(HBase.class);

    private static HBase hbase = new HBase();

    private HBase() {
        super(HBASE_TABLE_NAME);
    }

    public static HBase getInstance() {
        return hbase;
    }

    public int getNumberOfReferences(String sourceUrl) {
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        get.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES.getBytes());
        Result result;
        try {
            result = table.get(get);
            if (result != null) {
                byte[] res = result.getValue(HBASE_MARK_CF_NAME.getBytes(),
                        HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES.getBytes());
                if(res != null)
                    return Bytes.toInt(res);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return 0;
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
