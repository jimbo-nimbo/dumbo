package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.parser.Metadata;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ElasticSearchThreadTest {

    @Test
    public void createBuilder() {
    }

    @Test
    public void submitBulk() throws IOException {
        ElasticClientFactory elasticClientFactory = new ElasticClientFactory();
        TransportClient client = elasticClientFactory.createClient();
        ElasticSearchThread elasticSearchThread = new ElasticSearchThread(client);

        List<Metadata> metadataList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            metadataList.add(new Metadata("this is name!", "this is property!",
                    "fucking done " + i));
        }

        List<ElasticsearchWebpageModel> elasticsearchWebpageModels = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            elasticsearchWebpageModels.add(new ElasticsearchWebpageModel("this is article",
                    "this is title", metadataList));
        }

        elasticSearchThread.submitBulk(elasticsearchWebpageModels);
    }
}