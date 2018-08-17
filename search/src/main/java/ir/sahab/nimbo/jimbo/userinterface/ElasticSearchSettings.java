package ir.sahab.nimbo.jimbo.userinterface;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ElasticSearchSettings {

    protected final Properties properties;
    private final String clusterName;
    private final String hosts;
    private final String indexName;

    ElasticSearchSettings() {
        String resourceName = "elasticsearch.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            properties.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        clusterName = properties.getProperty("cluster.name");
        hosts = properties.getProperty("hosts");
        indexName = properties.getProperty("index.name");
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getHosts() {
        return hosts;
    }

    public String getIndexName() {
        return indexName;
    }
}
