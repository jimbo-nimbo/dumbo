package ir.sahab.nimbo.jimbo.userinterface;

import ir.sahab.nimbo.jimbo.elastic.Config;
import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import ir.sahab.nimbo.jimbo.hbase.HBase;
import org.elasticsearch.search.SearchHit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public class WebHandler {
    static JsonResultModel getAns(ArrayList<SearchHit> searchHits){
        final int SHOW_LIMIT = 10;
        final int SEARCH_LIMIT = Config.ES_RESULT_SIZE;
        int count = 0;
        ResultModel[] result = new ResultModel[SEARCH_LIMIT];
        ResultModel[] resultModels = new ResultModel[SHOW_LIMIT];
        JsonResultModel jsonResultModel = new JsonResultModel();
        ArrayList<Integer> index = new ArrayList<>();
        ArrayList<String> urls = new ArrayList<>();
        Long a = System.currentTimeMillis();
        for(SearchHit searchHit : searchHits){
            if(count < SEARCH_LIMIT){
                Map<String, Object> map = searchHit.getSourceAsMap();
                if(map.containsKey("url") && map.containsKey("title") && map.containsKey("content")) {
                    index.add(count);
                    String url = (String) map.get("url");
                    urls.add(url);
                    result[count] = new ResultModel();
                    result[count].setTitle((String)map.get("title"));
                    result[count].setUrl((String)map.get("url"));
                    String content = map.get("content").toString();
                    if (content.length() <= 500)
                        result[count].setDescription(content);
                    else {
                        result[count].setDescription(content.substring(0, 499));
                    }
                    count++;
                }
            }
        }
//        System.err.println(System.currentTimeMillis() - a);
        a = System.currentTimeMillis();
        Integer[] finalRefs = HBase.getInstance().getNumberOfReferences(urls);
        index.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return finalRefs[o2] - finalRefs[o1];
            }
        });
//        System.err.println(System.currentTimeMillis() - a);
        for(int i = 0; i < SHOW_LIMIT && i < searchHits.size(); i++) {
            resultModels[i] = new ResultModel(result[index.get(i)].getTitle(), result[index.get(i)].getUrl(),
                    result[index.get(i)].getDescription(), finalRefs[index.get(i)]);
        }
        jsonResultModel.setResultModels(resultModels);
        return jsonResultModel;
    }

    public static JsonResultModel webSearch(String searchText){
        ElasticClient.getInstance();
        ArrayList<SearchHit> ans = ElasticClient.getInstance().simpleElasticSearch(searchText);
        return getAns(ans);
    }

}
