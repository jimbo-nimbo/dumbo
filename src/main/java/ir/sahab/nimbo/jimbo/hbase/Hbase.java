package ir.sahab.nimbo.jimbo.hbase;

import com.google.protobuf.ServiceException;
import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Hbase {

    private static final String SITE_DIR = "hbase-site.xml";
    private static final String CORE_DIR = "core-site.xml";
    private static final String TABLE_NAME = "linkTable";
    private static final String cFAnchor = "Anchor";
    private static final String cFMeta = "Meta";
    private static final String cFTitle = "Title";
    private static final String cFText = "text";
    static Connection connection = null;
    TableName tableName;

    public Hbase() {
        tableName = TableName.valueOf(TABLE_NAME);
        Configuration config = HBaseConfiguration.create();
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(SITE_DIR)).getPath();
        config.addResource(new Path(path));
        path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(CORE_DIR)).getPath();
        config.addResource(new Path(path));
        boolean conn = true;
        while (conn) {
            try {
                connection = ConnectionFactory.createConnection(config);
                conn = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try (Admin admin = connection.getAdmin()){
            if(admin.tableExists(tableName)){
                initialize(admin);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void add(String row, String cF, String quantifier, String value){
        Put p = new Put(row.getBytes());
        p.addColumn(cF.getBytes(), quantifier.getBytes(), value.getBytes());
        try (Table table = Hbase.connection.getTable(tableName)){
            table.put(p);
        } catch (IOException e) {
            Logger.getInstance().logToFile("cant put to data base");
        }
    }

    private void initialize(Admin admin) {
        try {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(cFAnchor));
            desc.addFamily(new HColumnDescriptor(cFTitle));
            desc.addFamily(new HColumnDescriptor(cFMeta));
            desc.addFamily(new HColumnDescriptor(cFText));
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
