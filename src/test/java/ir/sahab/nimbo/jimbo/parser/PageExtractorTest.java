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
    private ArrayBlockingQueue<Document> queue;

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
        queue = new ArrayBlockingQueue<>(100);
        pageExtractor = new PageExtractorFactory(queue).getPageExtractor();
    }

    @Test
    public void extractMetadataTest() {
        List<Metadata> metadatas = pageExtractor.extractMetadata(doc);
        Assert.assertTrue(metadatas.contains(new Metadata("robots", "", "all")));
        Assert.assertTrue(metadatas.contains(new Metadata("googlebot", "", "index")));
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