package ir.sahab.nimbo.jimbo;


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

public class TestElasticsearch {
    public static void main(String[] args) throws IOException {

        //Create client
        final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("hitler", 9200, "http"),
                        new HttpHost("alexander", 9200, "http"),
                        new HttpHost("genghis", 9200, "http")));

        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            //Create Request
            final IndexRequest request = new IndexRequest(
                    "finaltest",
                    "_doc",
                    DigestUtils.md5Hex("https://www.test.com" + i));

            //Create document with ContentBuilder
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("content", "test document");
                builder.field("anchor", "tohi");
            }
            builder.endObject();

            //bind document to request
            request.source(builder);

            bulkRequest.add(request);
        }
        //submit request asynchronous
            client.bulkAsync(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    System.out.println("successful");
                }

                @Override
                public void onFailure(Exception e) {
                    System.out.println("failure");
                }
            });

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        client.close();
    }
}
