package ir.sahab.nimbo.jimbo.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchThreadFactory {

    private Settings settings;
    private List<Host> hosts;
    private Properties properties;

    static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    static int bulkSize;
    static String indexName;

    private final List<ElasticSearchThread> elasticSearchThreads = new ArrayList<>();

    public ElasticsearchThreadFactory(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue)
            throws ElasticCannotLoadException {
        try {
            setProperties();
            setSettings();
            setHosts();
        } catch (IOException e) {
            throw new ElasticCannotLoadException();
        }

        ElasticsearchThreadFactory.elasticQueue = elasticQueue;

        bulkSize = Integer.parseInt(properties.getProperty("bulk_size"));
        indexName = properties.getProperty("index.name");
    }

    public ElasticSearchThread createNewThread() throws IOException {
        ElasticSearchThread elasticSearchThread = new ElasticSearchThread(createClient());
        elasticSearchThreads.add(elasticSearchThread);
        return elasticSearchThread;
    }

    public void commitAll()
    {
    }

    /**
     * create it as an separate function for testing
     */
    private void setProperties() throws IOException {
        String resourceName = "elasticsearch-rss.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(resourceName);

        Properties props = new Properties();
        props.load(resourceStream);

        properties = props;
    }

    /**
     * there can be multiple hosts and we don't know the number until runtime
     * so read them from elasticsearch.properties file
     *
     * hosts are separated with # in properties file
     * example:
     * host1:port#host2:port
     *
     * package private for testing
     */
    void setHosts() {
        List<Host> hosts = new ArrayList<>();
        String hostsString = properties.getProperty("hosts");
        for (String hostString : hostsString.split("#")) {
            Host host = new Host(hostString.split(":")[0],
                    Integer.parseInt(hostString.split(":")[1]));
            hosts.add(host);
        }
        this.hosts = hosts;
    }

    /**
     * extract settings from properties object
     * read name from properties file
     *
     * set client.transport.sniff true for sending requests to master node //TODO: check this configuration
     * @see <a href="https://www.elastic.co/guide/en/
     *      elasticsearch/client/java-api/current/transport-client.html#transport-client">
     *      elasticsearch transport client</a>
     */
    private void setSettings() {
        String clusterName = properties.getProperty("cluster.name");
        settings = Settings.builder()
                .put("cluster.name", clusterName)
//                .put("client.transport.sniff", true)
                .build();
    }

    /**
     * get hosts from setHosts() because there may be multiple hosts
     * and it's configurable in elasticsearch.properties file in resources
     *
     * package private for testing
     */
    private TransportClient createClient() throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(settings);

        for (Host host : hosts) {
            client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(host.getHostName()), host.getPort()));
        }
        return client;
    }

    /**
     * class for encapsulate hosts
     */
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
