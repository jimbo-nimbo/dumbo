package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class Hbase {

    private static final String PROP_DIR = "hbase-site.xml";
    private static final String TABLE_NAME = "linkTable";

    private TableName tableName;
    private String family1 = "Family1";
    private String family2 = "Family2";
    private String row1 = "Row1";
    private String row2 = "Row2";
    private String val1 = "nimnimnimn1";
    private String val2 = "nimnimnimn2";
    private String val3 = "nimnimn3333";
    private String col1 = "col1";
    private String col2 = "col2";
    private String col3 = "col33";

    public Hbase() {
        tableName = TableName.valueOf(TABLE_NAME);
        Configuration config = HBaseConfiguration.create();
        String path = this.getClass().getClassLoader().getResource(PROP_DIR).getPath();
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
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            if (connection.getTable(tableName) == null) {
                Admin admin = connection.getAdmin();
                TableDescriptorBuilder tableDescriptorBuilder =
                        TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                        ColumnFamilyDescriptorBuilder.newBuilder(family1.getBytes());
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder2 =
                        ColumnFamilyDescriptorBuilder.newBuilder(family2.getBytes());
                //columnFamilyDescriptorBuilder.setValue(col1.getBytes(), val1.getBytes());
                //columnFamilyDescriptorBuilder2.setValue(col2.getBytes(), val2.getBytes());
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder2.build());
                admin.createTable(tableDescriptorBuilder.build());
            }
            Put p = new Put(row1.getBytes());
            p.addColumn(family1.getBytes(), col1.getBytes(), val3.getBytes());
            Table table = connection.getTable(TableName.valueOf("testTable"));
            System.err.println(table.getDescriptor().getValues().entrySet());
            table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
