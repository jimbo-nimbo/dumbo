package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.parser.Link;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static ir.sahab.nimbo.jimbo.main.Config.*;
import static org.junit.Assert.*;

public class HBaseTest {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTest.class);

    static final String STACKOVERFLOW = "https://stackoverflow.com";
    static final String JAVA_CODE = "https://examples.javacodegeeks.com";
    static URL STACKOVERFLOWURL;
    static URL JAVA_CODE_URL;
    @BeforeClass
    public static void setUpALL() throws MalformedURLException {
        STACKOVERFLOWURL = new URL(STACKOVERFLOW);
        JAVA_CODE_URL = new URL(JAVA_CODE);
        HBase.getInstance();
    }

    byte[] getMark(String sourceUrl, String qualifier) {
        Get get = new Get(HBase.getInstance().makeRowKey(sourceUrl).getBytes());
        try {
            return HBase.getInstance().table.get(get).getValue(HBASE_MARK_CF_NAME.getBytes(), qualifier.getBytes());
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Test
    public void getAndPutDataTest() throws MalformedURLException {
//        ArrayList<Link> links = new ArrayList<>();
//        String url = "http://www.test.com";
//        String anchor = "test";
//        links.add(new Link(new URL(url), anchor));
//        HBase.getInstance().putData(url, links);
//        byte[] res = HBase.getInstance().getData(url, "0link");
//        byte[] res2 = HBase.getInstance().getData(url, "0anchor");
//        assertEquals(url, new String(res));
//        assertEquals(anchor, new String(res2));

    }

    @Test
    public void putAndGetMarkTest() throws IOException {
        String url = "http://www.test.com";
        HBaseMarkModel hBaseMarkModel = new HBaseMarkModel(
                url, System.currentTimeMillis(), 100L, DigestUtils.md5Hex(url));
        HBase.getInstance().table.put(HBase.getInstance().getPutMark(hBaseMarkModel));
        HBaseMarkModel hBaseMarkModel1 = HBase.getInstance().getMark(url);
        assertEquals(url, hBaseMarkModel.getUrl());
        assertEquals(hBaseMarkModel.getBodyHash(), hBaseMarkModel1.getBodyHash());
        assertEquals(hBaseMarkModel.getDuration(), hBaseMarkModel1.getDuration());
        assertEquals(hBaseMarkModel.getLastSeen(), hBaseMarkModel1.getLastSeen());
    }

    @Test
    public void existData() {
        ArrayList<Link> links = new ArrayList<>();
//        Link link = new Link(STACKOVERFLOWURL, "anchor");
//        links.add(link);
//        HBase.getInstance().putData(STACKOVERFLOW, links);
//        assertTrue(HBase.getInstance().existData(STACKOVERFLOW));
//        assertFalse(HBase.getInstance().existData(JAVA_CODE));
    }

//    @Test
//    public void existMark() {
//        HBase.getInstance().putMark(STACKOVERFLOW);
//        assertTrue(HBase.getInstance().existMark(STACKOVERFLOW));
//        assertFalse(HBase.getInstance().existMark(JAVA_CODE));
//    }

//    @Test
//    public void putBulkData() throws MalformedURLException {
//        ArrayList<Link> arrayList = new ArrayList<>();
//        Link link = new Link(new URL("https://www.href.com"), "anchor");
//        arrayList.add(link);
//        for(int i = 0; i < 100; i++){
//            HBase.getInstance().putBulkData("https://www.test.com", arrayList);
//        }
//        assertEquals(100, HBase.getInstance().getBulkQueue().size());
//    }

//    @Test
//    public void putBulkMark() throws MalformedURLException {
//        HBase.getInstance().getBulkQueue().clear();
//        for(int i = 0; i < 100; i++){
//            HBase.getInstance().putBulkMark("https://www.test.com", "testVal");
//        }
//        assertEquals(100, HBase.getInstance().getBulkQueue().size());
//    }



    //@Test
//    public void singlePutHugeMarkImmediateNotTest(){
//        HBase hBase = HBase.getInstance();
//        for (int i = 0; i < 900; i++) {
//            hBase.putMark("https://www.nimac.com" + String.valueOf(i));
//            assertTrue(hBase.existMark("https://www.nimac.com" + String.valueOf(i)));
////            if(!hBase.existMark("https://www.nimac.com" + String.valueOf(i)))
////                System.err.println(i);
//        }
//        //Thread.sleep(10000);
//    }


    //@Test
    public void basicHbaseBenchmarkNotTest(){
        HBase hBase = HBase.getInstance();
        StringBuilder bigUrl = new StringBuilder("https://www.test.com");
        Random rand = new Random();
        final int size = 5000;
        ArrayList<Put> puts = new ArrayList<>();
        ArrayList<Put> puts2 = new ArrayList<>();
        for(int i = 0; i < 20; i++){
            bigUrl.append((char)(rand.nextInt() + 10));
        }
        for (int i = 0; i < size; i++) {
            Put put = new Put(bigUrl.toString().getBytes());
            put.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES, bigUrl.toString().getBytes());
            puts.add(put);
        }
        long b = System.currentTimeMillis();
        try {
            HBase.getInstance().table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println(System.currentTimeMillis() - b);
        System.err.println((System.currentTimeMillis() - b) / size);
        b = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            try {
                Put put = new Put(bigUrl.toString().getBytes());
                put.addColumn(HBASE_MARK_CF_NAME_BYTES, HBASE_MARK_Q_NAME_URL_BYTES, bigUrl.toString().getBytes());
                HBase.getInstance().table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.err.println(System.currentTimeMillis() - b);
        System.err.println((System.currentTimeMillis() - b) / size);
    }

//    public void benchmarkExistMarkNotTest(){
//        HBase hBase = HBase.getInstance();
//        long b = System.currentTimeMillis();
//        for (int i = 0; i < 300; i++) {
//            hBase.existMark("https://www.test.com" + String.valueOf(i));
//        }
//        System.err.println(System.currentTimeMillis() - b);
//    }

}