package ir.sahab.nimbo.jimbo.elasticSearch;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class ElasticClientFactoryTest {

    @Test
    public void getProperties() throws IOException {
        ElasticClientFactory elasticClientFactory = new ElasticClientFactory();
        Properties properties = elasticClientFactory.getProperties();

        assertEquals("sarb", properties.getProperty("cluster.name"));
        assertEquals("sarb", properties.getProperty("index.name"));
        assertEquals("localhost:9200", properties.getProperty("hosts"));

    }

    @Test
    public void getHosts() throws IOException {
        ElasticClientFactory elasticClientFactory = new ElasticClientFactory();
        List<ElasticClientFactory.Host> hosts = elasticClientFactory.getHosts(elasticClientFactory.getProperties());

        for (ElasticClientFactory.Host host: hosts){
            assertEquals("localhost", host.getHostName());
            assertEquals(9200, host.getPort());
        }

    }

    @Test
    public void getSettings() {
    }

    @Test
    public void createClient() throws IOException {
        ElasticClientFactory elasticClientFactory = new ElasticClientFactory();
        elasticClientFactory.createClient(elasticClientFactory.getProperties());
    }
}