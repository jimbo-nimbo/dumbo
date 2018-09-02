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

public class HBase{

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
            if(result != null) {
                byte[] url = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES);
                byte[] lseen = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_LAST_SEEN_BYTES);
                byte[] dur = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_SEEN_DURATION_BYTES);
                byte[] hash = result.getValue(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_CONTENT_HASH_BYTES);
                if (url == null && lseen != null && dur != null && hash != null) {
                    hBaseMarkModel = new HBaseMarkModel(Bytes.toString(url),
                            Bytes.toLong(lseen),
                            Bytes.toLong(dur),
                            Bytes.toString(hash));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }


    public void putMark(String sourceUrl) {
        Long value = System.currentTimeMillis();
        byte[] sourceBytes = Bytes.toBytes(sourceUrl);
        Put p = new Put(makeRowKey(sourceUrl).getBytes());
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_LAST_SEEN_BYTES,
                Bytes.toBytes(value));
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_SEEN_DURATION_BYTES,
                HBASE_MARK_DEFAULT_SEEN_DURATION_BYTES);
        p.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES,
                sourceBytes);
        p.addColumn(HBASE_DATA_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES,
                sourceBytes);
        try {
            table.put(p);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
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

    @SuppressWarnings("Duplicates")
    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(HBASE_DATA_CF_NAME));
            desc.addFamily(new HColumnDescriptor(HBASE_MARK_CF_NAME));
            //TODO region bandy
            admin.createTable(desc);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    String reverseUrl(URL url) {
        //return url.getProtocol() + "://" + reverseDomain(url.getHost()) + url.getPath();
        return reverseDomain(url.getHost()) + getHash(url.getPath());
    }

    private static String getHash(String inp) {
        return DigestUtils.md5Hex(inp);
    }

    String makeRowKey(String row){
        return getHash(row);
    }

    Table getTable() {
        return table;
    }

    @SuppressWarnings("Duplicates")
    ResultScanner scanData(List<byte[]> qulifiers){

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

    @SuppressWarnings("Duplicates")
    ResultScanner scanColumnFamily(List<byte[]> columnFamily){
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

    public String getContentHash(String sourceUrl){
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        get.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_CONTENT_HASH.getBytes());
        Result result;
        try {
            result = table.get(get);
            if (result != null) {
                byte[] res = result.getValue(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_CONTENT_HASH.getBytes());
                if(res != null){
                    return Bytes.toString(res);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return "";
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

    String getSeenDur(String sourceUrl){
        Get get = new Get(makeRowKey(sourceUrl).getBytes());
        get.addColumn(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_SEEN_DURATION.getBytes());
        Result result;
        try {
            result = table.get(get);
            if (result != null) {
                byte[] res = result.getValue(HBASE_MARK_CF_NAME.getBytes(), HBASE_MARK_Q_NAME_SEEN_DURATION.getBytes());
                if(res != null){
                    return Bytes.toString(res);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }
}
