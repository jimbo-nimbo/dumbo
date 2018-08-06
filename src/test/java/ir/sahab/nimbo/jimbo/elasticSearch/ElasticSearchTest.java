package ir.sahab.nimbo.jimbo.elasticSearch;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ElasticSearchTest {

    private ElasticSearch elasticSearch;

    @Before
    public void initialize()
    {
        try {
            elasticSearch = new ElasticSearch();
        } catch (ElasticCannotLoadException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getProperties() {
        try {
            String expectedHosts = elasticSearch.getProperties().getProperty("hosts");
            assertEquals(expectedHosts, "localhost:9200#localhost:9200");

            String ecpectedClusterName = elasticSearch.getProperties().getProperty("cluster.name");
            assertEquals(ecpectedClusterName, "jimbo");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createClient() {
    }

    @Test
    public void getSettings() {
    }

    @Test
    public void getHosts() {
    }
}