package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Objects;

public class Hbase {

    private static final String PROP_DIR = "hbase-site.xml";
    private static final String TABLE_NAME = "linkTable";
    static Connection connection = null;
    TableName tableName;
    static final String family1 = "Family1";
    private static final String family2 = "Family2";

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
                //
            }
        }
        try{
            connection = ConnectionFactory.createConnection(config);
            try {
                connection.getTable(tableName);
            } catch (TableNotFoundException e){
                Admin admin = connection.getAdmin();
                TableDescriptorBuilder tableDescriptorBuilder =
                        TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                        ColumnFamilyDescriptorBuilder.newBuilder(family1.getBytes());
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder2 =
                        ColumnFamilyDescriptorBuilder.newBuilder(family2.getBytes());
                columnFamilyDescriptorBuilder.setValue("col1".getBytes(), "val1".getBytes());
                columnFamilyDescriptorBuilder2.setValue("col2".getBytes(), "val2".getBytes());
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder2.build());
                admin.createTable(tableDescriptorBuilder.build());
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
