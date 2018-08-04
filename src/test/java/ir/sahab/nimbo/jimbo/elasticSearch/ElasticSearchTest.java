package ir.sahab.nimbo.jimbo.elasticSearch;

import org.elasticsearch.client.transport.TransportClient;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ElasticSearchTest
{
    private ElasticSearch elasticSearch;
    @BeforeClass
    public void createInstance() throws IOException
    {
        elasticSearch = new ElasticSearch();

    }

    @Test
    public void createClient()
    {

    }

    @Test
    public void getHosts()
    {

    }
}