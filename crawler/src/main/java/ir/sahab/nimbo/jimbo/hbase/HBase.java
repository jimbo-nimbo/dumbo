package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.parser.Link;
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
import java.util.Objects;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class HBase extends AbstractHBase {

    private static HBase hbase = new HBase();

    private HBase() {
        super(HBASE_TABLE_NAME);
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

    public static HBase getInstance() {
        return hbase;
    }

    /**
     * @param hBaseDataModel
     */

    Put getPutData(HBaseDataModel hBaseDataModel) {
        Put p;
        p = new Put(makeRowKey(hBaseDataModel.getUrl()).getBytes());
        Link link;
        for (int i = 0; i < hBaseDataModel.getLinks().size(); i++) {
            link = hBaseDataModel.getLinks().get(i);
            p.addColumn(HBASE_DATA_CF_NAME_BYTES,
                    (String.valueOf(i) + "link").getBytes(), link.getHref().getBytes());
            p.addColumn(HBASE_DATA_CF_NAME_BYTES,
                    (String.valueOf(i) + "anchor").getBytes(), link.getText().getBytes());
        }
        return p;
    }

    Put getPutMark(HBaseMarkModel markModel){
        byte[] sourceBytes = Bytes.toBytes(markModel.getUrl());
        Put p = new Put(makeRowKey(markModel.getUrl()).getBytes());
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_LAST_SEEN_BYTES,
                Bytes.toBytes(markModel.getLastSeen()));
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_SEEN_DURATION_BYTES,
                Bytes.toBytes(markModel.getDuration()));
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES,
                sourceBytes);
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_CONTENT_HASH_BYTES,
                Bytes.toBytes(markModel.getBodyHash()));
        p.addColumn(HBASE_DATA_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES,
                sourceBytes);
        return p;
    }

    public HBaseMarkModel getMark(String sourceUrl){
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        get.addFamily(HBASE_MARK_CF_NAME_BYTES);
        HBaseMarkModel hBaseMarkModel;
        try {
            Result result = table.get(get);
            if (result != null) {
                byte[] url = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES);
                byte[] lseen = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_LAST_SEEN_BYTES);
                byte[] dur = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_SEEN_DURATION_BYTES);
                byte[] hash = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_CONTENT_HASH_BYTES);
                if (url != null && lseen != null && dur != null && hash != null) {
                    hBaseMarkModel = new HBaseMarkModel(Bytes.toString(url),
                            Bytes.toLong(lseen),
                            Bytes.toLong(dur),
                            Bytes.toString(hash));
                    return hBaseMarkModel;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    Table getTable() {
        return table;
    }
}
