package ir.sahab.nimbo.jimbo.elasticsearch;

import ir.sahab.nimbo.jimbo.ElasticClientBuilder;
import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHandler.class);

    private final ElasticsearchSetting elasticsearchSetting;

    private static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private final TransportClient client;

    public static int successfulSubmit = 0;
    public static int failureSubmit = 0;

    public ElasticsearchHandler(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                                ElasticsearchSetting elasticsearchSetting) throws UnknownHostException {

        this.elasticsearchSetting = elasticsearchSetting;
        client = ElasticClientBuilder.build();
        ElasticsearchHandler.elasticQueue = elasticQueue;
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
                    successfulSubmit++;
                } else {
                    failureSubmit++;
                }
            } catch (IOException e) {
                failureSubmit++;
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
