package ir.sahab.nimbo.jimbo.elastic;


import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ElasticClientTest {
    @Test
    public void simpleSearchTest() {
        List<SearchHit> results = ElasticClient.getInstance().simpleSearchInElasticForWebPage("sag");
        Assert.assertEquals(results.get(1).getSourceAsMap().get("title"), "sag khar");
    }

    @Test
    public void advancedSearchTest() {
        ArrayList<String> l1 = new ArrayList<>();
        l1.add("sag");
        ArrayList<String> l2 = new ArrayList<>();
        l2.add("nooeb");
        List<SearchHit> results = ElasticClient.getInstance().advancedSearchInElasticForWebPage(
                l1, l2, new ArrayList<>());
        Assert.assertEquals(results.get(1).getSourceAsMap().get("title"), "for");
    }

}
