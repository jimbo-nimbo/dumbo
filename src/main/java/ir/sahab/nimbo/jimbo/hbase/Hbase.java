package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Hbase {

    private static final String PROP_DIR = "hbase-site.xml";
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
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource(PROP_DIR)).getPath();
        config.addResource(new Path(path));
        boolean conn = true;
        while (conn) {
            try {
                HBaseAdmin.available(config);
                conn = false;
            } catch (IOException e) {
                Logger.getInstance().logToFile("cant read Hbase Config");
            }
        }
        try {
            connection = ConnectionFactory.createConnection(config);
            try {
                connection.getTable(tableName);
            } catch (TableNotFoundException e) {
                initialize();
            }
        } catch (IOException e) {
            Logger.getInstance().logToFile("cant create Hbase connection");
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

    private void initialize() {
        try (Admin admin = connection.getAdmin()) {
            TableDescriptorBuilder tableDescriptorBuilder =
                    TableDescriptorBuilder.newBuilder(tableName);
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFAnchor.getBytes()).build());
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFMeta.getBytes()).build());
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFTitle.getBytes()).build());
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(cFText.getBytes()).build());
            //columnFamilyDescriptorBuilder.setValue("col1".getBytes(), "val1".getBytes());
            //columnFamilyDescriptorBuilder2.setValue("col2".getBytes(), "val2".getBytes());
            tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            Logger.getInstance().logToFile(e.getMessage());
        }
    }
}
