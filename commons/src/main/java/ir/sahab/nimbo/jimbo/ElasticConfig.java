package ir.sahab.nimbo.jimbo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class ElasticConfig {
    private static final Logger logger = LoggerFactory.getLogger(ElasticConfig.class);

    public static final String CLUSTER_NAME;
    public static final String HOSTS;
    public static final int BULK_SIZE;
    public static final String INDEX_NAME;

    static {
        String resourceName = "elasticsearch.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        CLUSTER_NAME = props.getProperty("cluster.name");
        HOSTS = props.getProperty("hosts");
        BULK_SIZE = Integer.parseInt(props.getProperty("bulk_size"));
        INDEX_NAME = props.getProperty("index_name");
    }
}
