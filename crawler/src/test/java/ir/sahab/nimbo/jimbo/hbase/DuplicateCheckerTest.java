package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DuplicateCheckerTest {

    static final String STACKOVERFLOW = "https://stackoverflow.com";
    static final String JAVA_CODE = "https://examples.javacodegeeks.com";
    static URL STACKOVERFLOWURL;
    static URL JAVA_CODE_URL;
    static HBaseMarkModel JAVA_CODE_MARK_MODEL = new HBaseMarkModel(
            JAVA_CODE, System.currentTimeMillis(), 100L, DigestUtils.md5Hex(JAVA_CODE));
    @BeforeClass
    public static void setUpALL() throws MalformedURLException {
        STACKOVERFLOWURL = new URL(STACKOVERFLOW);
        JAVA_CODE_URL = new URL(JAVA_CODE);
        Metrics.getInstance().mockInit();
    }


    void addJava(){
        DuplicateChecker.getInstance().add(JAVA_CODE_MARK_MODEL);
    }

    @Test
    public void add() {
        addJava();
        assertTrue(DuplicateChecker.cache.estimatedSize() > 0);
        assertTrue(DuplicateChecker.arrayBlockingQueue.size() > 0);
    }

    @Test
    public void updateLastSeen() {
    }

    @Test
    public void take() {
    }

    @Test
    public void getShouldFetchTest() {
        try {
            HBaseMarkModel hBaseMarkModel = new HBaseMarkModel(
                    JAVA_CODE, System.currentTimeMillis(), Config.HBASE_DURATION_MIN, DigestUtils.md5Hex(JAVA_CODE));
            HBase.getInstance().table.put(HBase.getInstance().getPutMark(hBaseMarkModel));

            HBaseMarkModel hBaseMarkModel1 = DuplicateChecker.getInstance().getShouldFetchMarkModel(JAVA_CODE);
            System.err.println(hBaseMarkModel1.getUrl() + " " + hBaseMarkModel1.getLastSeen() + " " +
                    hBaseMarkModel1.getDuration() + " " + hBaseMarkModel1.getBodyHash());
            assertTrue(hBaseMarkModel1.getDuration() +
                    hBaseMarkModel1.getLastSeen() > System.currentTimeMillis());
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}