package ir.sahab.nimbo.jimbo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class Main {
    public static void main2(String[] args) throws UnknownHostException {

        TransportClient client = ElasticClientBuilder.build();
        QueryBuilder qb = matchAllQuery();
        BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client)
                .source(ElasticConfig.INDEX_NAME)
                .destination("testindex4")
                .get();
    }

    public static void main(String[] args) throws UnknownHostException {
        TransportClient client = ElasticClientBuilder.build();
        QueryBuilder qb = matchAllQuery();

        SearchResponse scrollResp = client.prepareSearch(ElasticConfig.INDEX_NAME)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(180, TimeUnit.SECONDS))
                .setQuery(qb)
                .setSize(500).get();

        int total = 0;

        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                System.out.println("\"" + hit.getId() + "\",");
//                Map<String, Object> source = hit.getSourceAsMap();
//                String url = (String) source.get("url");
//                String content = (String) source.get("content");
//                String title = (String) source.get("title");
//                String description = (String) source.get("description");
                total++;
            }
             System.out.println("Next... " + total);

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(180, TimeUnit.SECONDS))
                    .execute()
                    .actionGet();
        } while (scrollResp.getHits().getHits().length != 0);

        client.close();
    }
}
