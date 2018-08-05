package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;


public class HbaseTest {

    private Hbase hbase;

    @Test
    public void prepare() {
        hbase = new Hbase();
    }

    @Before
    public void setUp() {
        hbase = new Hbase();
    }

    @After
    public void tearDown() {
    }


    @Test
    public void simpleTest() {

//        Get get = new Get(row1.getBytes());
//        Put p = new Put(row1.getBytes());
//        p.addColumn(family1.getBytes(), col1.getBytes(), val3.getBytes());
//        get.addFamily(family1.getBytes());
//        Table table = null;
//        try {
//            table = Hbase.connection.getTable(hbase.tableName);
//            table.put(p);
//            Result result = table.get(get);
//            System.err.println(result.getCursor().getRow().toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        String r, cf, q, v;
        r = "row1";
        cf = "Anchor";
        q = "1";
        v = "click";
        hbase.add(r, cf, q, v);


    }
}
