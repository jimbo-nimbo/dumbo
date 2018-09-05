package ir.sahab.nimbo.jimbo.elasticsearch;

import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchHandlerTest {
    private ElasticsearchHandler elasticSearchHandler;
    private ArrayBlockingQueue<ElasticsearchWebpageModel> models;

    @Before
    public void init() throws UnknownHostException {
        models = new ArrayBlockingQueue<>(10000);
        elasticSearchHandler = new ElasticsearchHandler(models, new ElasticsearchSetting());
    }

//    @Test
//    public void submit() throws IOException {
//        List<ElasticsearchWebpageModel> list = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            list.add(new ElasticsearchWebpageModel("url " + i, "article " + i,
//                    "title " + i, "description " + i));
//        }
//        elasticSearchHandler.submit(list, "test1");
//    }

    @Test
    public void testRun() throws InterruptedException, ConnectException {
        elasticSearchHandler.runWorkers();
         for (int i = 0; i < new ElasticsearchSetting().getBulkSize(); i++) {
            models.put(new ElasticsearchWebpageModel("url " + i, "article " + i,
                    "title " + i, "description " + i));
        }
        Thread.sleep(10000);
    }
}