package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Hbase {

    private static final String SITE_DIR = "hbase-site.xml";
    private static final String CORE_DIR = "core-site.xml";
    private static final String TABLE_NAME = "siteTable";
    private static final String cFLink = "LinkAnchor";
    private static final String cFFlag = "Flag";
    private Connection connection = null;
    private Configuration config = null;
    private Admin admin = null;
    private Table table = null;
    TableName tableName;

    public Hbase() {
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
        try{
            admin = connection.getAdmin();
            if(!admin.tableExists(tableName)){
                initialize(admin);
            }
            table = connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void addLinkCF(String sourceUrl, String destUrl, String anchor){

        String revUrl = sourceUrl;
        String hashDest = destUrl;
        String val = destUrl + ":" + anchor;
        Put p = new Put(revUrl.getBytes());
        p.addColumn(cFLink.getBytes(), hashDest.getBytes(), val.getBytes());
        try {
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(cFLink));
            desc.addFamily(new HColumnDescriptor(cFFlag));
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
