package hbase_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;


public class HbaseApi {

    private TableName table1 = TableName.valueOf("testTable");
    private String family1 = "Family1";
    private String family2 = "Family2";

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
        try(Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            HTableDescriptor desc = new HTableDescriptor(table1);
            desc.addFamily(new HColumnDescriptor(family1));
            desc.addFamily(new HColumnDescriptor(family2));
            admin.createTable(desc);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
