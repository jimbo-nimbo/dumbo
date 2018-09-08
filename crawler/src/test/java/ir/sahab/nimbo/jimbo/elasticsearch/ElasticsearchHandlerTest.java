package ir.sahab.nimbo.jimbo.elasticsearch;

import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import ir.sahab.nimbo.jimbo.parser.WebPageModel;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchHandlerTest {
    private ElasticsearchHandler elasticSearchHandler;
    private ArrayBlockingQueue<ElasticsearchWebpageModel> models;

    @Before
    public void init() throws UnknownHostException, ConnectException {
        models = new ArrayBlockingQueue<>(10000);
        Metrics.getInstance().initialize(new ArrayBlockingQueue<List<String>>(1000),
                new ArrayBlockingQueue< WebPageModel >(1000),
                models,
                new ArrayBlockingQueue< HBaseDataModel >(1000));
        elasticSearchHandler = new ElasticsearchHandler(models, new ElasticsearchSetting());
        elasticSearchHandler.runWorkers();
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
        final ElasticsearchSetting elasticsearchSetting = new ElasticsearchSetting();
        for (int i = 0; i < elasticsearchSetting.getBulkSize()* elasticsearchSetting.getNumberOfThreads() + 100; i++) {
            models.put(new ElasticsearchWebpageModel("url " + i, "article " + i,
                    "title " + i, "description " + i));
        }
        Thread.sleep(10000);
    }
}