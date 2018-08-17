package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.parser.Link;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class HbaseBulkThreadTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void run() throws InterruptedException {
        HBase hBase = HBase.getInstance();
        Thread myThread = new Thread(new HbaseBulkThread());
        myThread.start();
        ArrayList<Link> arrayList = new ArrayList<>();
        Link link = new Link("https://www.href.com", "anchor");
        arrayList.add(link);
        for (int i = 0; i < 900; i++) {
            hBase.putBulkData(new HBaseDataModel("https://www.test.com" + String.valueOf(i), arrayList));
        }
        //Thread.sleep(10000);
        for(int i = 0; i < 900; i++) {
            if(!hBase.existData("https://www.test.com" + String.valueOf(i)))
                System.err.println(i);
        }
    }
}