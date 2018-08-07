package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Logger;
import ir.sahab.nimbo.jimbo.parser.Link;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Hbase {

    private static final String SITE_DIR = "hbase-site.xml";
    private static final String CORE_DIR = "core-site.xml";
    private static final String TABLE_NAME = "siteTable";
    private static final String CF_LINK = "LinkAnchor";
    private static final String CF_FLAG = "Flag";
    private static Hbase hbase = null;
    TableName tableName;
    private Connection connection = null;
    private Configuration config = null;
    private Admin admin = null;
    private Table table = null;

    private Hbase() {
        tableName = TableName.valueOf(TABLE_NAME);
        config = HBaseConfiguration.create();
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(SITE_DIR)).getPath();
        config.addResource(new Path(path));
        //path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(CORE_DIR)).getPath();
        //config.addResource(new Path(path));
        boolean conn = true;
        while (conn) {
            try {
                connection = ConnectionFactory.createConnection(config);
                conn = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                initialize(admin);
            }
            table = connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    synchronized public static Hbase getInstance() {
        if (hbase == null) {
            hbase = new Hbase();
        }
        return hbase;
    }

    /**
     * @param sourceUrl : should be in reverse pattern
     * @param
     * @param
     */

    public void putIntoLinkCF(String sourceUrl, List<Link> links) {
//        String strNum = String.valueOf(num);
//        String val = destUrl + ":" + anchor;
//        Put p = new Put(sourceUrl.getBytes());
//        p.addColumn(CF_LINK.getBytes(), strNum.getBytes(), val.getBytes());
//        try {
//            table.put(p);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void putIntoFlagCF() {

    }

    public void getFromLinkCF(String url){

    }

    public void getFromFlagCF(String url){

    }

    public boolean existInLinkCF(String url){
        return false;
    }
    public boolean existInFlagCF(String url){
        return false;
    }


    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(CF_LINK));
            desc.addFamily(new HColumnDescriptor(CF_FLAG));
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
            Logger.getInstance().logToFile(e.getMessage());
        }
    }
}
