package ir.sahab.nimbo.jimbo;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticClientBuilder;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticConfig;
import ir.sahab.nimbo.jimbo.hbase.HBaseInputScanner;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class Main {
    public static void reindex(String[] args) throws UnknownHostException {

        TransportClient client = ElasticClientBuilder.buildTransport();
        QueryBuilder qb = matchAllQuery();
        BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client)
                .source(ElasticConfig.INDEX_NAME)
                .destination("testindex4")
                .get();
    }

    public static void transportScroll(String[] args) throws UnknownHostException {
        TransportClient client = ElasticClientBuilder.buildTransport();
        QueryBuilder qb = matchAllQuery();

        SearchResponse scrollResp = client.prepareSearch(ElasticConfig.INDEX_NAME)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(180, TimeUnit.SECONDS))
                .setQuery(qb)
                .setSize(500).get();

        int total = 0;

        do {
            List<String> docIds = new ArrayList<>();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                docIds.add(hit.getId());

            }
            System.out.println("Next... " + total);

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(180, TimeUnit.SECONDS))
                    .execute()
                    .actionGet();
        } while (scrollResp.getHits().getHits().length != 0);

        client.close();
    }

    public static void clientTest(String[] args) throws IOException {
        RestClient restClient = ElasticClientBuilder.buildRest();

//        Request request = new Request("POST", "/" + ElasticConfig.INDEX_NAME + "/_search");
//        request.addParameter("scroll", "1m");
//        Response response = restClient.performRequest(request);

        //System.out.println(EntityUtils.toString(response.getEntity()));
//        JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
        //System.out.println(jsonObject.get("tagline"));

        restClient.close();
    }

    /*public void getTermVector(String id) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        String jsonString =
                "{\n" +
                        "  \"fields\" : [\"content\"],\n" +
                        "  \"offsets\" : true,\n" +
                        "  \"payloads\" : true,\n" +
                        "  \"positions\" : true,\n" +
                        "  \"term_statistics\" : true,\n" +
                        "  \"field_statistics\" : true\n" +
                        "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response =
                restClient.performRequest("GET", "/" + index + "/_doc/" + id + "/_termvectors", params, entity);
        JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
        JSONObject jsonArray = jsonObject.getJSONObject("term_vectors").getJSONObject("content").getJSONObject("terms");
        for (String key : jsonArray.keySet()) {
            System.out.println(key + "=" + jsonArray.get(key)); // to get the value
        }
    }*/

    public static void main(String[] args) throws IOException, InterruptedException {
        RestClient restClient = ElasticClientBuilder.buildRest();
        HBaseInputScanner.getInstance().initializeScan();
        Map<String, String> urlMap = new HashMap<>();
        while (HBaseInputScanner.getInstance().hasNext()) {
            List<String> urls = HBaseInputScanner.getInstance().nextBulk();
            StringBuilder docIds = new StringBuilder();
            for (String url : urls) {
                String docId = DigestUtils.md5Hex(url);
                urlMap.put(docId, url);
                docIds.append('"');
                docIds.append(docId);
                docIds.append("\",\n");
            }
            if (docIds.length() >= 2) {
                docIds.deleteCharAt(docIds.length() - 1);
                docIds.deleteCharAt(docIds.length() - 1);
            }

            // Getting multi term vectors
            String requestBody = "{\n" +
                    "  \"ids\": [" + docIds.toString() + "],\n" +
                    "  \"parameters\": {\n" +
                    "    \"fields\": [\n" +
                    "      \"content\"\n" +
                    "    ],\n" +
                    "    \"term_statistics\": true,\n" +
                    "    \"field_statistics\": false,\n" +
                    "    \"positions\": false,\n" +
                    "    \"offsets\": false,\n" +
                    "    \"filter\": {\n" +
                    "      \"max_num_terms\": 5,\n" +
                    "      \"min_term_freq\": 1,\n" +
                    "      \"min_doc_freq\": 1\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            Request request = new Request("POST", "/" + ElasticConfig.INDEX_NAME
                    + "/_doc/_mtermvectors");
            request.setJsonEntity(requestBody);
            Response response = restClient.performRequest(request);
            JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
            JSONArray docs = jsonObject.getJSONArray("docs");

            for (int i = 0; i < docs.length(); i++) {
                JSONObject doc = docs.getJSONObject(i);
                String docId = doc.getString("_id");
                boolean found = doc.getBoolean("found");
                if (!found) {
                    continue;
                }
                List<String> keywords = new ArrayList<>();
                JSONObject terms = doc.getJSONObject("term_vectors").getJSONObject("content").getJSONObject("terms");
                for (String key : terms.keySet()) {
                    keywords.add(key);
                }
                System.out.println(urlMap.get(docId) + keywords);
            }

            // Put keywords to HBase
            // ToDo...
        }

        restClient.close();
    }
}
