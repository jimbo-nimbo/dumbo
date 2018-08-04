package ir.sahab.nimbo.jimbo.parser;

import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.*;

public class PageExtractorTest {

    private PageExtractor pageExtractor;

    public PageExtractorTest() {
        String resourceName = "extractor-test.html";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Document doc;
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)){
            doc = Jsoup.parse(resourceStream, "UTF-8", "");
        } catch (IOException e) {
            doc = new Document("");
            e.printStackTrace();
        }
        pageExtractor = new PageExtractor(doc);
    }

    @Test
    public void extractMetadataTest() {
        List<Metadata> metadatas = pageExtractor.extractMetadata();
        Assert.assertTrue(metadatas.contains(new Metadata("robots", "", "all")));
        Assert.assertTrue(metadatas.contains(new Metadata("googlebot", "", "index")));
    }

    @Test
    public void extractLinksTest() throws MalformedURLException {
        List<Link> links = pageExtractor.extractLinks();
        Assert.assertTrue(links.contains(new Link(new URL("http://google.com"), "Google is the best")));
        Assert.assertTrue(links.contains(new Link(new URL("http://yahoo.com"), "Yahoo is the worst")));
        Assert.assertTrue(links.contains(new Link(new URL("http://facebook.com"),
                "Facebook is somewhere in the middle")));
    }

    // TODO: test kafka


}