package ir.sahab.nimbo.jimbo.elasticsearch;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticSearchThreadTest {

    @Test
    public void createBuilder() {
    }

    @Test
    public void submitBulk() throws IOException, ElasticCannotLoadException {
        ElasticsearchThreadFactory elasticsearchThreadFactory =
                new ElasticsearchThreadFactory(new ArrayBlockingQueue<>(10000));

        ElasticSearchThread elasticSearchThread = elasticsearchThreadFactory.createNewThread();

        List<ElasticsearchWebpageModel> elasticsearchWebpageModels = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            elasticsearchWebpageModels.add(new ElasticsearchWebpageModel("this si url", "this is article",
                    "this is title", "this is description"));
        }

        elasticSearchThread.submitBulk(elasticsearchWebpageModels);
    }
}