package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.main.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchThread implements Runnable{

    private TransportClient client;

    private final String indexName;
    private final int bulkSize;

    private XContentBuilder builder;

    ElasticSearchThread(TransportClient client) throws IOException {
        this.client = client;

        indexName = ElasticsearchThreadFactory.indexName;
        bulkSize = ElasticsearchThreadFactory.bulkSize;

    }

    /**
     * use only one builder for minimize memory use
     */
    void buildBuilder(ElasticsearchWebpageModel model) throws IOException {

        builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("content", model.getArticle())
                .field("title", model.getTitle())
                .field("description", model.getDescription());

        builder.endObject();
    }

    /**
     * this function is for ease of change id of items in elasticsearch
     */
    private String getId(ElasticsearchWebpageModel elasticsearchWebpageModel)
    {
        Integer i = elasticsearchWebpageModel.getUrl().hashCode();
        System.out.println(i);
        return i.toString();
    }

    /**
     * function to accelerate submitting process
     *
     * @return
     *      return true if submit is successful
     */
    boolean submitBulk(List<ElasticsearchWebpageModel> models) throws IOException {

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (ElasticsearchWebpageModel model: models){
            buildBuilder(model);
            bulkRequestBuilder.add(
                    client.prepareIndex(indexName, "_doc", getId(model)).setSource(builder)
            );
        }

        return !bulkRequestBuilder.get().hasFailures();
    }

    /**
     * close client connection at the end of program
     * because we have only this connection so far
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        client.close();
    }

    @Override
    public void run() {
        List<ElasticsearchWebpageModel> list = new ArrayList<>();
        int t = 0;
        while(true) {
            if (t < ElasticsearchThreadFactory.bulkSize) {
                try {
                    list.add(ElasticsearchThreadFactory.elasticQueue.take());
                    t++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                t = 0;
                try {
                    if (submitBulk(list)) {
                        Logger.getInstance().infoLog("Submitted "
                                + ElasticsearchThreadFactory.bulkSize + "of records" + "to elasticsearch!");
                    } else {
                        Logger.getInstance().warnLog("Cannot submit records to elasticsearch!");
                    }
                    list.clear();
                } catch (IOException e) {
                    Logger.getInstance().warnLog("Cannot submit records to elasticsearch!(due exception)");
                }
            }
        }
    }
}
