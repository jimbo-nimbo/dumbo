package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.assertTrue;

public class DuplicateCheckerTest {

    static final String STACKOVERFLOW = "https://stackoverflow.com";
    static final String JAVA_CODE = "https://examples.javacodegeeks.com";
    static URL STACKOVERFLOWURL;
    static URL JAVA_CODE_URL;
    @BeforeClass
    public static void setUpALL() throws MalformedURLException {
        STACKOVERFLOWURL = new URL(STACKOVERFLOW);
        JAVA_CODE_URL = new URL(JAVA_CODE);
        Metrics.getInstance().mockInit();
    }

    @Test
    public void add() {
        //DuplicateChecker.getInstance().add();
    }

    @Test
    public void updateLastSeen() {
    }

    @Test
    public void take() {
    }

    @Test
    public void getInstance() {
    }

    @Test
    public void getCache() {
    }

    @Test
    public void getArrayBlockingQueue() {
    }
}