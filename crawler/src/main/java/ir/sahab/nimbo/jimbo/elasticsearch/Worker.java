package ir.sahab.nimbo.jimbo.elasticsearch;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Worker implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final ElasticsearchSetting elasticsearchSetting;

    private static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private final RestHighLevelClient client;

    public Worker(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                  ElasticsearchSetting elasticsearchSetting) {
        this.elasticsearchSetting = elasticsearchSetting;
        this.elasticQueue = elasticQueue;

        this.client = new RestHighLevelClient(
        RestClient.builder(
                new HttpHost("hitler", 9200, "http"),
                new HttpHost("genghis", 9200, "http")));
    }

    @Override
    public void run() {

        final int bulkSize = elasticsearchSetting.getBulkSize();
        final String indexName = elasticsearchSetting.getIndexName();
        List<ElasticsearchWebpageModel> models = new ArrayList<>();
        while (true) {
            Metrics.getInstance().markElasticWorkerNumberOfCyclesDone();
            models.clear();
            for (int i = 0; i < bulkSize; i++) {
                try {
                    models.add(elasticQueue.take());
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
            Timer.Context elasticSearchWorkerJobsRequestsTimeContext =
                    Metrics.getInstance().elasticSearchWorkerJobsRequestsTime();
            try {
                submit(models, indexName);
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                elasticSearchWorkerJobsRequestsTimeContext.stop();
            }
        }
    }


    void submit(List<ElasticsearchWebpageModel> models, String indexName) throws IOException {
        XContentBuilder builder;
        BulkRequest bulkRequest = new BulkRequest();
        for (ElasticsearchWebpageModel model : models) {

            final IndexRequest request = new IndexRequest(
                indexName,
                "_doc",
                getId(model.getUrl()));

            builder = XContentFactory.jsonBuilder();
            builder.startObject()
                    .field("url", model.getUrl())
                    .field("content", model.getArticle())
                    .field("title", model.getTitle())
                    .field("description", model.getDescription())
                    .endObject();

            request.source(builder);

            bulkRequest.add(request);
        }
        BulkResponse bulk = client.bulk(bulkRequest);
        if(bulk.hasFailures()){
            Metrics.getInstance().markElasticSubmitFailure();
        } else {
            Metrics.getInstance().markElasticSubmitSuccess();
        }
//        client.bulkAsync(bulkRequest, new ActionListener<BulkResponse>() {
//            @Override
//            public void onResponse(BulkResponse bulkItemResponses) {
//                Metrics.getInstance().markElasticSubmitSuccess();
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                Metrics.getInstance().markElasticSubmitFailure();
//                logger.warn(e.getMessage());
//            }
//        });
    }

    private String getId(String url) {
        return DigestUtils.md5Hex(url);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        client.close();
    }
}
