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
    private static final String CF_DATA = "Data";
    private static final String CF_MARK = "Mark";

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

    public void putData(String sourceUrl, List<Link> links) {
        Put p = new Put(sourceUrl.getBytes());
        Link link;
        for(int i = 0; i < links.size(); i++){
            link = links.get(i);
            p.addColumn(CF_DATA.getBytes(), String.valueOf(i).getBytes(), (link.getText() + ":" + link.getHref()).getBytes());
        }
        try {
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putMark(String sourceUrl, String value) {
        Put p = new Put(sourceUrl.getBytes());
        p.addColumn(CF_MARK.getBytes(), "qualif".getBytes(), value.getBytes());
        try {
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getData(String sourceUrl){

    }

    public void getMark(String sourceUrl){

    }

    public boolean existData(String sourceUrl){
        return false;
    }
    public boolean existMark(String sourceUrl){
        return false;
    }


    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(CF_DATA));
            desc.addFamily(new HColumnDescriptor(CF_MARK));
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
