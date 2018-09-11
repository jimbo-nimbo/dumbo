package ir.sahab.nimbo.jimbo;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class TestElasticsearch {
    public static void main(String[] args) throws IOException {

        //Create client
        final RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("hitler", 9200, "http"),
                        new HttpHost("genghis", 9200, "http"),
                        new HttpHost("alexander", 9200, "http")));

        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            //Create Request
            final IndexRequest request = new IndexRequest(
                    "anchorstest",
                    "_doc",
                    DigestUtils.md5Hex("http://www.test" + i + ".com"));

            //Create document with ContentBuilder
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("content", "www.test" + i + ".com");
                builder.field("anchor", "tohi");
            }
            builder.endObject();

            //bind document to request
            request.source(builder);

            final IndexResponse index = client.index(request, RequestOptions.DEFAULT);
            System.out.println(index.status());
//            bulkRequest.add(request);
        }
        //submit request asynchronous
//        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
//            @Override
//            public void onResponse(BulkResponse bulkItemResponses) {
//                System.out.println("successful+");
//                System.out.println(bulkItemResponses.status());
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                System.out.println("failure");
//            }
//        });

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        client.close();
    }
}
