package hbase_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;


public class HbaseApi {

    private TableName table1 = TableName.valueOf("testTable");
    private String family1 = "Family1";
    private String family2 = "Family2";
    private String row1 = "Row1";
    private String row2 = "Row2";
    private String val1 = "nimnimnimn1";
    private String val2 = "nimnimnimn2";
    private String col1 = "col1";
    private String col2 = "col2";


    @Test
    public void basicCommand(){
        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));
        try {
            HBaseAdmin.available(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table1);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family1.getBytes());
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder2 = ColumnFamilyDescriptorBuilder.newBuilder(family2.getBytes());
            columnFamilyDescriptorBuilder.setValue(col1.getBytes(),val1.getBytes());
            columnFamilyDescriptorBuilder2.setValue(col2.getBytes(),val2.getBytes());
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder2.build());
            admin.createTable(tableDescriptorBuilder.build());
            //TableDescriptorBuilder tableDescriptorBuilder12 = TableDescriptorBuilder.newBuilder()
            //Put p = new Put(row1.getBytes());
            //p.addColumn(family1.getBytes(), col1.getBytes(), val1.getBytes());

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
