package ir.sahab.nimbo.jimbo.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Get;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static ir.sahab.nimbo.jimbo.main.Config.HBASE_MARK_CF_NAME;
import static org.junit.Assert.assertEquals;

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

}