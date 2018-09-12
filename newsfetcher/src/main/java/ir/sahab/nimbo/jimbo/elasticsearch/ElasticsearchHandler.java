package ir.sahab.nimbo.jimbo.elasticsearch;

import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
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
        client = ElasticClientBuilder.buildTransport();
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
                    .field("date", new Date().toString())
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

    public ArrayList<String> findTrendWords() {
        String date = new Date().toString().substring(0, 6);
        Map<String, String> params = Collections.emptyMap();
        String jsonString = "{\n" +
                "  \"query\": {\n" +
                "    \"range\": {\n" +
                "      \"date\": {\n" +
                "        \"gte\": \""+date+"\",\n" +
                "        \"lte\": \""+date+"\",\n" +
                "        \"format\": \"MMM dd\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        ArrayList<String> links = new ArrayList<>();
        try {
//            JSONArray hits = new JSONObject(EntityUtils.toString(response.getEntity())).getJSONObject("hits").getJSONArray("hits");
//            for (Object bucket : hits) {
//                links.add(((JSONObject) bucket).getJSONObject("_source").getString("pageLink"));
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return links;
    }

    public ArrayList<SearchHit> findTrendNews() {
        return ElasticClient.getInstance().jimboElasticSearch(new ArrayList<>(), new ArrayList<>(), findTrendWords());
    }
}
