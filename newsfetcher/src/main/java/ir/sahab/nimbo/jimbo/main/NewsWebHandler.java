package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Map;

public class NewsWebHandler {
    static JsonNewsResultModel getAns(ArrayList<SearchHit> searchHits){
        final int LIMIT = 10;
        int count = 0;
        JsonNewsResultModel jsonResultModel = new JsonNewsResultModel();
        NewsResultModel[] newsResultModels = new NewsResultModel[LIMIT];
        for(SearchHit searchHit : searchHits){
            if(count < LIMIT){
                newsResultModels[count] = new NewsResultModel();
                Map<String, Object> map = searchHit.getSourceAsMap();
                newsResultModels[count].setTitle((String) map.get("title"));
                        newsResultModels[count].setUrl((String) map.get("url"));
                String content = map.get("content").toString();
                if(content.length() <= 500)
                    newsResultModels[count].setDescription(content);
                else {
                    newsResultModels[count].setDescription(content.substring(0, 499));
                }
                count++;
            }
        }
        int resSize = 0;
        while (resSize < newsResultModels.length && newsResultModels[resSize] != null){
            resSize++;
        }
        NewsResultModel[] finalResult = new NewsResultModel[resSize];
        for(int i = 0; i < resSize; i++){
            finalResult[i] = newsResultModels[i];
        }
        jsonResultModel.setNewsResultModels(finalResult);
        return jsonResultModel;
    }

    public static JsonNewsResultModel newsSearch(String searchText){
        ElasticClient.getInstance();
        ArrayList<SearchHit> ans = ElasticClient.getInstance().simpleElasticSearch(searchText);
        return getAns(ans);
    }

}
