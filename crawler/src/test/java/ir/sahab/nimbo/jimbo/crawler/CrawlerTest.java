package ir.sahab.nimbo.jimbo.crawler;

import org.junit.BeforeClass;
import org.junit.Test;

public class CrawlerTest {
    private Crawler crawler;

    @BeforeClass
    public void createInstance()
    {
        try {
            crawler = new Crawler(new CrawlerSetting());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void checkConstructor()
    {

    }
}