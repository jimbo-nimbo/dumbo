package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static ir.sahab.nimbo.jimbo.hbase.Hbase.family1;
import static javax.swing.UIManager.get;

public class HbaseTest {

  private Hbase hbase;
  private String row1 = "Row1";
  private String row2 = "Row2";
  private String val1 = "nimnimnimn1";
  private String val2 = "nimnimnimn2";
  private String val3 = "nimnimn3333";
  private String col1 = "col1";
  private String col2 = "col2";
  private String col3 = "col33";

  @Test
  public void prepare(){
      hbase = new Hbase();
  }
  @Before
  public void setUp() {
    hbase = new Hbase();
  }

  @After
  public void tearDown() {}



  @Test
  public void simpleTest() {

    Get get = new Get(row1.getBytes());
    Put p = new Put(row1.getBytes());
    p.addColumn(family1.getBytes(), col1.getBytes(), val3.getBytes());
    get.addFamily(family1.getBytes());
    Table table = null;
    try {
      table = Hbase.connection.getTable(hbase.tableName);
      table.put(p);
      Result result = table.get(get);
      System.err.println(result.getCursor().getRow().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
