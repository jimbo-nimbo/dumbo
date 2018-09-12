package ir.sahab.nimbo.jimbo.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ElasticClientSearch {

    private static ElasticClientSearch ourInstance = new ElasticClientSearch();
    private RestHighLevelClient client;

    public static ElasticClientSearch getInstance() {
//        System.err.println("instance");
        return ourInstance;
    }

    private ElasticClientSearch() {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ConfigSearch.ES_HOSTS.get(0).getHostName(),
                                ConfigSearch.ES_HOSTS.get(0).getPort(),
                                ConfigSearch.ES_SCHEME),
                        new HttpHost(ConfigSearch.ES_HOSTS.get(1).getHostName(),
                                ConfigSearch.ES_HOSTS.get(1).getPort(),
                                ConfigSearch.ES_SCHEME),
                        new HttpHost(ConfigSearch.ES_HOSTS.get(2).getHostName(),
                                ConfigSearch.ES_HOSTS.get(2).getPort(),
                                ConfigSearch.ES_SCHEME)
                        )
                        .setRequestConfigCallback(
                                requestConfigBuilder ->
                                        requestConfigBuilder
                                                .setConnectTimeout(ConfigSearch.ES_CONNECTION_TIMEOUT)
                                                .setSocketTimeout(ConfigSearch.ES_SOCKET_TIMEOUT))
                        .setMaxRetryTimeoutMillis(ConfigSearch.ES_MAXRETRY_TIMEOUT));
    }

    public ArrayList<SearchHit> simpleElasticSearch(String mustFind) {
//        System.err.println("hi");
        ArrayList<String> simpleQuery = new ArrayList<>();
//        System.err.println("by");
        simpleQuery.add(mustFind);
//        System.err.println("ti");
        return jimboElasticSearch(simpleQuery, new ArrayList<>(), new ArrayList<>());
    }

    public ArrayList<SearchHit> jimboElasticSearch(
            ArrayList<String> mustFind,
            ArrayList<String> mustNotFind,
            ArrayList<String> shouldFind) {
        SearchRequest searchRequest = new SearchRequest(ConfigSearch.ES_INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        String fields[] = {"url", "content", "title", "description"};
        for (String phrase : mustFind) {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(
                            phrase, fields)
                            .type(MultiMatchQueryBuilder.Type.PHRASE);
            for (String field : fields)
                multiMatchQueryBuilder.field(field, ConfigSearch.getScoreField(field));
            boolQuery.must(multiMatchQueryBuilder);
        }
        for (String phrase : mustNotFind) {
            boolQuery.mustNot(
                    QueryBuilders.multiMatchQuery(
                            phrase, fields)
                            .type(MultiMatchQueryBuilder.Type.PHRASE));
        }
        for (String phrase : shouldFind) {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(
                            phrase, fields)
                            .type(MultiMatchQueryBuilder.Type.PHRASE);
            for (String field : fields)
                multiMatchQueryBuilder.field(field, ConfigSearch.getScoreField(field));
            boolQuery.should(multiMatchQueryBuilder);
        }
        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.size(ConfigSearch.ES_RESULT_SIZE);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            return new ArrayList<SearchHit>(Arrays.asList(searchHits));
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        } catch (RuntimeException r) {
            r.printStackTrace();
            return null;
        }
    }

}
