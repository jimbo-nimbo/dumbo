package ir.sahab.nimbo.jimbo.elasticsearch;

import ir.sahab.nimbo.jimbo.ElasticClientBuilder;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticWorker extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ElasticWorker.class);

    private final ElasticsearchSetting elasticsearchSetting;

    private static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private final TransportClient client;

    public ElasticWorker(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                                ElasticsearchSetting elasticsearchSetting) throws UnknownHostException {
        this.elasticsearchSetting = elasticsearchSetting;
        client = ElasticClientBuilder.build();
        this.elasticQueue = elasticQueue;
    }

    @Override
    public void run() {

        final int bulkSize = elasticsearchSetting.getBulkSize();
        final String indexName = elasticsearchSetting.getIndexName();
        List<ElasticsearchWebpageModel> models = new ArrayList<>();
        while (true) {
            models.clear();
            for (int i = 0; i < bulkSize; i++) {
                try {
                    models.add(elasticQueue.take());
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            try {
                if (submit(models, indexName)) {
                    Metrics.getInstance().markElasticSubmitSuccess();
                } else {
                    Metrics.getInstance().markElasticSubmitFailure();
                }
            } catch (IOException e) {
                Metrics.getInstance().markElasticSubmitFailure();
                logger.error(e.getMessage());
            }
        }
    }


    boolean submit(List<ElasticsearchWebpageModel> models, String indexName) throws IOException {
        XContentBuilder builder;
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (ElasticsearchWebpageModel model : models) {
            builder = XContentFactory.jsonBuilder();
            builder.startObject()
                    .field("url", model.getUrl())
                    .field("content", model.getArticle())
                    .field("title", model.getTitle())
                    .field("description", model.getDescription())
                    .endObject();
            bulkRequestBuilder.add(
                    client.prepareIndex(indexName, "_doc", getId(model.getUrl())).setSource(builder));
        }

        return !bulkRequestBuilder.get().hasFailures();
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
