package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Logger;
import ir.sahab.nimbo.jimbo.parser.Link;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class HBase {

    private static HBase hbase = new HBase();
    private TableName tableName;
    Table table = null;
    private ArrayBlockingQueue<Put> bulkData = new ArrayBlockingQueue<>(HBASE_BULK_CAPACITY);

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
                e.printStackTrace();
            }
        }
        try {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                initialize(admin);
            }
            table = connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBase getInstance() {
        return hbase;
    }

    ArrayBlockingQueue<Put> getBulkQueue() {
        return bulkData;
    }

    /**
     * @param sourceUrl : should be in reverse pattern
     * @param
     * @param
     */

    private Put getPutData(String sourceUrl, List<Link> links) {
        Put p;
        try {
            p = new Put(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
        Link link;
        for (int i = 0; i < links.size(); i++) {
            link = links.get(i);
            p.addColumn(HBASE_DATA_CF_NAME.getBytes(),
                    (String.valueOf(i) + "link").getBytes(), link.getHref().toString().getBytes());
            p.addColumn(HBASE_DATA_CF_NAME.getBytes(),
                    (String.valueOf(i) + "anchor").getBytes(), link.getText().getBytes());
        }
        return p;
    }

    public void putBulkData(String sourceUrl, List<Link> links) {
        Put p = getPutData(sourceUrl, links);
        try {
            if (links.size() > 0)
            bulkData.put(p);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void putData(String sourceUrl, List<Link> links) {
        Put p = getPutData(sourceUrl, links);
        try {
            if (links.size() > 0)
                table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putBulkMark(String sourceUrl, String value) {
        Put p = null;
        try {
            p = new Put(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        p.addColumn(HBASE_MARK_CF_NAME.getBytes(), "qualif".getBytes(), value.getBytes());
        //TODO
        try {
            bulkData.put(p);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void putMark(String sourceUrl, String value) {
        Put p = null;
        try {
            p = new Put(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        p.addColumn(HBASE_MARK_CF_NAME.getBytes(), "qualif".getBytes(), value.getBytes());
        //TODO
        try {
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] getData(String sourceUrl, String qualifier) {
        Get get = null;
        try {
            get = new Get(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        Result result = null;
        try {
            return table.get(get).getValue(HBASE_DATA_CF_NAME.getBytes(), qualifier.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    public byte[] getMark(String sourceUrl, String qualifier) {
        Get get = null;
        try {
            get = new Get(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        Result result = null;
        try {
            return table.get(get).getValue(HBASE_MARK_CF_NAME.getBytes(), qualifier.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    boolean existData(String sourceUrl) {
        Get get = null;
        try {
            get = new Get(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        Result result = null;
        try {
            result = table.get(get);
            if (result != null) {
                NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(HBASE_DATA_CF_NAME.getBytes());
                if (navigableMap != null && !navigableMap.isEmpty()) {
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean existMark(String sourceUrl) {
        Get get = null;
        try {
            get = new Get(reverseUrl(new URL(sourceUrl)).getBytes());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        Result result = null;
        try {
            result = table.get(get);
            if (result != null) {
                NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(HBASE_MARK_CF_NAME.getBytes());
                if (navigableMap != null && !navigableMap.isEmpty()) {
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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
        } catch (IndexOutOfBoundsException e1) {
            Logger.getInstance().debugLog(e1.getMessage());
        }
        return stringBuilder.toString();
    }

    private static String getHash(String inp) {
        return DigestUtils.md5Hex(inp);
    }

    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(HBASE_DATA_CF_NAME));
            desc.addFamily(new HColumnDescriptor(HBASE_MARK_CF_NAME));
            //TODO reagiob bandy
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
            Logger.getInstance().debugLog(e.getMessage());
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
            e.printStackTrace();
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
            e.printStackTrace();
        }
        return results;



    }

    Table getTable() {
        return table;
    }
}
