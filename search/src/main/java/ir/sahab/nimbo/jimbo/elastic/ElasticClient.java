package ir.sahab.nimbo.jimbo.elastic;

import ir.sahab.nimbo.jimbo.userinterface.ElasticSearchSettings;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class ElasticClient {

    private static ElasticClient ourInstance;
    private static String index;


    public static ElasticClient getInstance() {
        if(ourInstance == null)
            ourInstance = new ElasticClient();
        return ourInstance;
    }

    private ElasticClient() {
//        initialize();
    }

    public ArrayList<String> simpleSearchInElasticForWebPage(String mustFind) {
        ArrayList<String> simpleQuery = new ArrayList<>();
        simpleQuery.add(mustFind);
        return advancedSearchInElasticForWebPage(simpleQuery, new ArrayList<>(), new ArrayList<>());
    }


    public ArrayList<String> advancedSearchInElasticForWebPage(
            ArrayList<String> mustFind,
            ArrayList<String> mustNotFind,
            ArrayList<String> shouldFind) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String phrase : mustFind) {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(
                            phrase, "text", "title", "description", "keywords", "anchors")
                            .type(MultiMatchQueryBuilder.Type.PHRASE);
            multiMatchQueryBuilder.field("text", 5);
            multiMatchQueryBuilder.field("title", 2);
            multiMatchQueryBuilder.field("description", 1);
            multiMatchQueryBuilder.field("keywords", 1);
            multiMatchQueryBuilder.field("anchors", 2);
            boolQuery.must(multiMatchQueryBuilder);
        }
        for (String phrase : mustNotFind) {
            boolQuery.mustNot(
                    QueryBuilders.multiMatchQuery(
                            phrase, "text", "title", "description", "keywords", "anchors")
                            .type(MultiMatchQueryBuilder.Type.PHRASE));
        }
        for (String phrase : shouldFind) {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(
                            phrase, "text", "title", "description", "keywords", "anchors")
                            .type(MultiMatchQueryBuilder.Type.PHRASE);
            multiMatchQueryBuilder.field("text", 5);
            multiMatchQueryBuilder.field("title", 2);
            multiMatchQueryBuilder.field("description", 1);
            multiMatchQueryBuilder.field("keywords", 1);
            multiMatchQueryBuilder.field("anchors", 2);
            boolQuery.should(multiMatchQueryBuilder);
        }
        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.storedField("url");
        searchRequest.source(searchSourceBuilder);
//        SearchResponse searchResponse = client.search(searchRequest);
//        SearchHits hits = searchResponse.getHits();
//        SearchHit[] searchHits = hits.getHits();
//        ArrayList<String> answer = new ArrayList<>();
//        for (SearchHit hit : searchHits) {
//            answer.add(hit.field("url").getValue().toString());
//        }
//        return answer;
        return null;
    }

}
