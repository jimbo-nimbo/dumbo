package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import org.jsoup.Jsoup;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ParserTest {

    private int arraySize = 10000;
    private ArrayBlockingQueue<WebPageModel> webPages;
    private ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;
    private ArrayBlockingQueue<HBaseDataModel> hbaseQueue;

    private ParserSetting parserSetting;
    private Parser parser;

    @Before
    public void initialize() {
        webPages = new ArrayBlockingQueue<>(arraySize);
        elasticQueue = new ArrayBlockingQueue<>(arraySize);
        parserSetting = new ParserSetting(4);
        hbaseQueue = new ArrayBlockingQueue<>(arraySize);
        AtomicInteger atomicInteger = new AtomicInteger(8);
        parser = new Parser(webPages, elasticQueue, hbaseQueue, parserSetting, atomicInteger);
    }

    @Test
    public void runWorkers() throws IOException, InterruptedException {
        final int linkCount = 20;
        for (int i = 0; i < linkCount; i++) {
            webPages.add(new WebPageModel(Jsoup.connect("https://en.wikipedia.org/wiki/" + i)
                    .validateTLSCertificates(false).get().html(), "https://en.wikipedia.org/wiki/" + i));
            System.out.println(i);
        }
        System.out.println("run workers!");
        parser.runWorkers();

        for (int i = 0; i < linkCount; i++) {
            final ElasticsearchWebpageModel take = elasticQueue.take();
            System.out.println(take.getDescription());
        }
    }
}