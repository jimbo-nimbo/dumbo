package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.parser.Metadata;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class ElasticSearchThread implements Runnable{
    private TransportClient client;

    private static AtomicLong id = new AtomicLong(0L);

    public ElasticSearchThread(TransportClient client) {
        this.client = client;
    }

    XContentBuilder createBuilder(ElasticsearchWebpageModel model) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("article", model.getArticle())
                .field("title", model.getTitle());
        List<Metadata> metadataList = model.getMetadataList();
        for (int i = 0; i < metadataList.size(); i++) {
            Metadata metadata = metadataList.get(i);
            builder.field("meta" + i, metadata.getContent());
        }
        return builder.endObject();
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
            XContentBuilder builder = createBuilder(model);
            //todo remove sarb
            bulkRequestBuilder.add(
                    client.prepareIndex("sarb", "_doc", Long.toString(id.incrementAndGet()))
                    .setSource(builder)
            );
        }

        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        return bulkItemResponses.hasFailures();
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

    }
}
