package ir.sahab.nimbo.jimbo.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class ElasticSearchSettings {

    private RestHighLevelClient client;
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
        client =
                new RestHighLevelClient(
                        RestClient.builder(new HttpHost(getHosts().get(0).getHostName(), getHosts().get(0).getPort(), "http"), new HttpHost(getHosts().get(1).getHostName(), getHosts().get(1).getPort(), "http"))
                                .setRequestConfigCallback(
                                        requestConfigBuilder ->
                                                requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(600000))
                                .setMaxRetryTimeoutMillis(600000));
    }

    public String getClusterName() {
        return clusterName;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public List<Host> getHosts() {
        String hostsString = this.hosts;
        List<Host> hosts = new ArrayList<>();
        for (String hostString : hostsString.split("#")) {
            Host host = new Host(hostString.split(":")[0],
                    Integer.parseInt(hostString.split(":")[1]));
            hosts.add(host);
        }

        return hosts;
    }

    public String getIndexName() {
        return indexName;
    }

    class Host {
        private String hostName;
        private int port;

        Host(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
        }

        String getHostName() {
            return hostName;
        }

        int getPort() {
            return port;
        }
    }
}
