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
import java.net.URL;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class HBase implements DuplicateChecker {

    private static final Logger logger = LoggerFactory.getLogger(HBase.class);

    private static HBase hbase = new HBase();
    private TableName tableName;
    Table table = null;

    // TODO: remove all the unnecessary functions
    private HBase() {
        tableName = TableName.valueOf(HBASE_TABLE_NAME);
        Configuration config = HBaseConfiguration.create();
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(HBASE_SITE_DIR)).getPath();
        config.addResource(new Path(path));
        path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(HBASE_CORE_DIR)).getPath();
        config.addResource(new Path(path));
        boolean conn = true;
        Connection connection = null;
        while (conn) {
            try {
                connection = ConnectionFactory.createConnection(config);
                conn = false;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        try {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                initialize(admin);
            }
            table = connection.getTable(tableName);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
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
            p.addColumn(HBASE_DATA_CF_NAME.getBytes(),
                    (String.valueOf(i) + "link").getBytes(), link.getHref().getBytes());
            p.addColumn(HBASE_DATA_CF_NAME.getBytes(),
                    (String.valueOf(i) + "anchor").getBytes(), link.getText().getBytes());
        }
        return p;
    }

    public void putData(HBaseDataModel hBaseDataModel) {
        Put p = getPutData(hBaseDataModel);
        try {
            if (hBaseDataModel.getLinks().size() > 0) {
                table.put(p);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void putMark(String sourceUrl) {
        String value = String.valueOf(System.currentTimeMillis());
        Put p = new Put(makeRowKey(sourceUrl).getBytes());
        p.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_LAST_SEEN.getBytes(), value.getBytes());
        p.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_SEEN_DURATION.getBytes(),
                HBASE_MARK_DEFAULT_SEEN_DURATION.getBytes());
        p.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_URL.getBytes(), sourceUrl.getBytes());
        p.addColumn(HBASE_DATA_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_URL.getBytes(), sourceUrl.getBytes());
        try {
            table.put(p);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public byte[] getData(String sourceUrl, String qualifier) {
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        try {
            return table.get(get).getValue(HBASE_DATA_CF_NAME.getBytes(), qualifier.getBytes());
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }

    }

    public byte[] getMark(String sourceUrl, String qualifier) {
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        try {
            return table.get(get).getValue(HBASE_MARK_CF_NAME.getBytes(), qualifier.getBytes());
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }

    }

    boolean existData(String sourceUrl) {
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        Result result;
        try {
            result = table.get(get);
            if (result != null) {
                NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(HBASE_DATA_CF_NAME.getBytes());
                if (navigableMap != null && !navigableMap.isEmpty()) {
                    return true;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    /**
     *
     * why? because there is two type: type one : those which not seen at all, two : those which should update
     */
    public boolean shouldFetch(String sourceUrl) {
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        get.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_LAST_SEEN.getBytes());
        get.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_SEEN_DURATION.getBytes());
        Result result;
        try {
            result = table.get(get);
            if (result != null) {
                byte[] res = result.getValue(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_LAST_SEEN.getBytes());
                byte[] resDur = result.getValue(
                        HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_SEEN_DURATION.getBytes());
                if(res != null && resDur != null){
                    Long lastseen = Long.valueOf(Bytes.toString(res));
                    Long dur = Long.valueOf(Bytes.toString(resDur));
                    if(System.currentTimeMillis() > lastseen + dur)
                        return true;
                    return false;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return true;
    }


    boolean existMark(String sourceUrl) {
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        get.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_LAST_SEEN.getBytes());
        Result result;
        try {
            result = table.get(get);
            if (result != null) {
                NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(HBASE_MARK_CF_NAME.getBytes());
                if (navigableMap != null && !navigableMap.isEmpty()) {
                    return true;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }


    String reverseUrl(URL url) {
        //return url.getProtocol() + "://" + reverseDomain(url.getHost()) + url.getPath();
        return reverseDomain(url.getHost()) + getHash(url.getPath());
    }

    private String reverseDomain(String domain) {
        StringBuilder stringBuilder = new StringBuilder();
        String[] res = domain.split("\\.");
        try {
            stringBuilder.append(res[res.length - 1]);
            for (int i = 1; i < res.length; i++) {
                stringBuilder.append("." + res[res.length - 1 - i]);
            }
        } catch (IndexOutOfBoundsException e) {
            logger.error(e.getMessage());
        }
        return stringBuilder.toString();
    }

    private static String getHash(String inp) {
        return DigestUtils.md5Hex(inp);
    }

    public int getNumberOfReferences(String sourceUrl){
        Scan scan = new Scan(makeRowKey(sourceUrl).getBytes());
        HBASE_MARK_CF_NAME.getBytes();
        HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES.getBytes();
        scan.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES.getBytes());
        ResultScanner resultScanner;
        try {
            resultScanner = table.getScanner(scan);
            for(Result result : resultScanner) {
                if (result != null) {
                    return Bytes.toInt(result.getValue(HBASE_MARK_CF_NAME.getBytes(),
                            HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES.getBytes()));
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return 0;
    }
    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(HBASE_DATA_CF_NAME));
            desc.addFamily(new HColumnDescriptor(HBASE_MARK_CF_NAME));
            //TODO region bandy
            admin.createTable(desc);
//            TableDescriptorBuilder tableDescriptorBuilder =
//                    TableDescriptorBuilder.newBuilder(tableName);
//            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
//            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFAnchor.getBytes()).build());
//            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFMeta.getBytes()).build());
//            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFTitle.getBytes()).build());
//            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFText.getBytes()).build());
//            //columnFamilyDescriptorBuilder.setValue("col1".getBytes(), "val1".getBytes());
//            //columnFamilyDescriptorBuilder2.setValue("col2".getBytes(), "val2".getBytes());
//            tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
//            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public ResultScanner scanData(List<byte[]> qulifiers){

        Scan scan = new Scan();
        ResultScanner results = null;
        for(byte[] bytes : qulifiers) {
            scan.addColumn(HBASE_DATA_CF_NAME.getBytes(), bytes);
        }
        try {
            results = table.getScanner(scan);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return results;
    }
    public ResultScanner scanColumnFamily(List<byte[]> columnFamily){

        Scan scan = new Scan();
        ResultScanner results = null;
        for(byte[] bytes : columnFamily) {
            scan.addFamily(bytes);
        }
        try {
            results = table.getScanner(scan);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return results;
    }

    String makeRowKey(String row){
        return getHash(row);
    }

    Table getTable() {
        return table;
    }
}
