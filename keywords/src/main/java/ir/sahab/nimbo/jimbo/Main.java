package ir.sahab.nimbo.jimbo;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticClientBuilder;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticConfig;
import ir.sahab.nimbo.jimbo.hbase.HBaseInputScanner;
import ir.sahab.nimbo.jimbo.hbase.HBaseOutput;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
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
                HBaseOutput.getInstance().appendPut(urlMap.get(docId), keywords);
            }

            // Put keywords to HBase
            HBaseOutput.getInstance().sendPuts();
        }

        restClient.close();
    }
}
