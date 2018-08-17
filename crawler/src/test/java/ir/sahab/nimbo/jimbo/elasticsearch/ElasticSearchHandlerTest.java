package ir.sahab.nimbo.jimbo.elasticsearch;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ElasticSearchHandlerTest {
    private ElasticSearchHandler elasticSearchHandler;
    private ArrayBlockingQueue<ElasticsearchWebpageModel> models;

    @Before
    public void init() throws UnknownHostException {
        models = new ArrayBlockingQueue<>(10000);
        elasticSearchHandler = new ElasticSearchHandler(models, new ElasticsearchSetting());
    }

    @Test
    public void submit() throws IOException {
        List<ElasticsearchWebpageModel> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new ElasticsearchWebpageModel("url " + i, "article " + i,
                    "title " + i, "description " + i));
        }
        elasticSearchHandler.submit(list, "test1");
    }

    @Test
    public void testRun() throws InterruptedException {
        new Thread(elasticSearchHandler).start();
         for (int i = 0; i < new ElasticsearchSetting().getBulkSize(); i++) {
            models.put(new ElasticsearchWebpageModel("url " + i, "article " + i,
                    "title " + i, "description " + i));
        }
        Thread.sleep(10000);
    }
}