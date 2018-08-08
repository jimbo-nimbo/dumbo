package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.parser.Link;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class HbaseTest {


    static final String STACKOVERFLOW = "https://stackoverflow.com";
    static final String JAVA_CODE = "https://examples.javacodegeeks.com";
    static URL STACKOVERFLOWURL;
    static URL JAVA_CODE_URL;
    @BeforeClass
    public static void setUpALL() throws MalformedURLException {
        STACKOVERFLOWURL = new URL(STACKOVERFLOW);
        JAVA_CODE_URL = new URL(JAVA_CODE);
    }
    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getInstance() {
    }

    @Test
    public void getAndPutDataTest() throws MalformedURLException {
        ArrayList<Link> links = new ArrayList<>();
        String url = "http://www.test.com";
        links.add(new Link(new URL(url), "test"));
        Hbase.getInstance().putData(url, links);
        byte[] res = Hbase.getInstance().getData(url, "0");
        assertEquals("test:" + url, new String(res));

    }

    @Test
    public void putAndGetMarkTest() throws MalformedURLException {
        ArrayList<Link> links = new ArrayList<>();
        String url = "http://www.test.com";
        Hbase.getInstance().putMark(url, "test");
        byte[] res = Hbase.getInstance().getMark(url, "qualif");
        assertEquals("test", new String(res));
    }

    @Test
    public void existData() {
        ArrayList<Link> links = new ArrayList<>();
        Link link = new Link(STACKOVERFLOWURL, "anchor");
        links.add(link);
        Hbase.getInstance().putData(STACKOVERFLOW, links);
        assertTrue(Hbase.getInstance().existData(STACKOVERFLOW));
        assertFalse(Hbase.getInstance().existData(JAVA_CODE));
    }

    @Test
    public void existMark() {
        Hbase.getInstance().putMark(STACKOVERFLOW, "value");
        assertTrue(Hbase.getInstance().existMark(STACKOVERFLOW));
        assertFalse(Hbase.getInstance().existMark(JAVA_CODE));
    }
    @Test
    public void revUrlTest() throws MalformedURLException {
        //assertEquals("https://com.google", Hbase.getInstance().reverseUrl(new URL("https://google.com")));
        assertEquals("https://com.google.www", Hbase.getInstance().reverseUrl(new URL("https://www.google.com")));
        assertEquals("https://com.google.dev.www", Hbase.getInstance().reverseUrl(new URL("https://www.dev.google.com")));
        //assertEquals("https://com.google/test/test", Hbase.getInstance().reverseUrl(new URL("https://google.com/test/test")));
        assertEquals("https://com.google.www/test/test", Hbase.getInstance().reverseUrl(new URL("https://www.google.com/test/test")));
        assertEquals("https://com.google.dev.www/test/test", Hbase.getInstance().reverseUrl(new URL("https://www.dev.google.com/test/test")));
    }
}