package ir.sahab.nimbo.jimbo.elastic;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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

public class ElasticClient {

    private static ElasticClient ourInstance;
    private static RestHighLevelClient client;
    private static ElasticSearchSettings elasticSearchSettings;


    public static ElasticClient getInstance() {
        if(ourInstance == null)
            ourInstance = new ElasticClient();
        return ourInstance;
    }


    private ElasticClient() {
        elasticSearchSettings = new ElasticSearchSettings();
        client = elasticSearchSettings.getClient();
    }

    public ArrayList<SearchHit> simpleSearchInElasticForWebPage(String mustFind) {
        ArrayList<String> simpleQuery = new ArrayList<>();
        simpleQuery.add(mustFind);
        return advancedSearchInElasticForWebPage(simpleQuery, new ArrayList<>(), new ArrayList<>());
    }


    public ArrayList<SearchHit> advancedSearchInElasticForWebPage(
            ArrayList<String> mustFind,
            ArrayList<String> mustNotFind,
            ArrayList<String> shouldFind) {
        SearchRequest searchRequest = new SearchRequest(elasticSearchSettings.getIndexName());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String phrase : mustFind) {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(
                            phrase, "url", "content", "title", "description")
                            .type(MultiMatchQueryBuilder.Type.PHRASE);
            multiMatchQueryBuilder.field("url", 1);
            multiMatchQueryBuilder.field("content", 5);
            multiMatchQueryBuilder.field("title", 2);
            multiMatchQueryBuilder.field("description", 1);
            boolQuery.must(multiMatchQueryBuilder);
        }
        for (String phrase : mustNotFind) {
            boolQuery.mustNot(
                    QueryBuilders.multiMatchQuery(
                            phrase, "url", "content", "title", "description")
                            .type(MultiMatchQueryBuilder.Type.PHRASE));
        }
        for (String phrase : shouldFind) {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(
                            phrase, "url", "content", "title", "description")
                            .type(MultiMatchQueryBuilder.Type.PHRASE);
            multiMatchQueryBuilder.field("url", 1);
            multiMatchQueryBuilder.field("content", 2);
            multiMatchQueryBuilder.field("title", 5);
            multiMatchQueryBuilder.field("description", 1);
            boolQuery.should(multiMatchQueryBuilder);
        }
        searchSourceBuilder.query(boolQuery);
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
