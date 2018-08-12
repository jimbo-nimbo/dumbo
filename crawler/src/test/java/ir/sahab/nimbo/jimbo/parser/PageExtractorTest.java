package ir.sahab.nimbo.jimbo.parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PageExtractorTest {

    private PageExtractor pageExtractor;
    private Document doc;

    @Before
    public void init() {
        String resourceName = "extractor-test.html";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            doc = Jsoup.parse(resourceStream, "UTF-8", "");
        } catch (IOException e) {
            doc = new Document("");
            e.printStackTrace();
        }
        ArrayBlockingQueue<Document> queue = new ArrayBlockingQueue<>(100);
        pageExtractor = new PageExtractorFactory(queue, new ArrayBlockingQueue<>(1000)).getPageExtractor();
    }

    @Test
    public void extractDescriptionMetaTest() {
        String metadata = pageExtractor.extractDescriptionMeta(doc);
        Assert.assertEquals("hello to you too", metadata);
    }

    @Test
    public void extractLinksTest() throws MalformedURLException {
        List<Link> links = pageExtractor.extractLinks(doc);
        Assert.assertTrue(links.contains(new Link(new URL("http://google.com"),
                "Google is the best")));
        Assert.assertTrue(links.contains(new Link(new URL("http://yahoo.com"),
                "Yahoo is the worst")));
        Assert.assertTrue(links.contains(new Link(new URL("http://facebook.com"),
                "Facebook is somewhere in the middle")));
    }

}