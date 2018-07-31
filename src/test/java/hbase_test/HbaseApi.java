package hbase_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;


public class HbaseApi {

    private TableName table1 = TableName.valueOf("Table1");
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
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
