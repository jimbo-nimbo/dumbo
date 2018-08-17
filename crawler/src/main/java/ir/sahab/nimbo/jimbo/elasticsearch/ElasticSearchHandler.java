package ir.sahab.nimbo.jimbo.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticSearchHandler implements Runnable {

    private final ElasticsearchSetting elasticsearchSetting;

    private static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private final TransportClient client;

    public static int successfulSubmit = 0;
    public static int failureSubmit = 0;

    public ElasticSearchHandler(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                                ElasticsearchSetting elasticsearchSetting) throws UnknownHostException {

        this.elasticsearchSetting = elasticsearchSetting;
        client = createClient();
        ElasticSearchHandler.elasticQueue = elasticQueue;
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
    private List<Host> readHosts() {
        List<Host> hosts = new ArrayList<>();
        String hostsString = elasticsearchSetting.getHosts();
        for (String hostString : hostsString.split("#")) {
            Host host = new Host(hostString.split(":")[0],
                    Integer.parseInt(hostString.split(":")[1]));
            hosts.add(host);
        }

        return hosts;
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
    private Settings readSettings() {
        String clusterName = elasticsearchSetting.getClusterName();
        return Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();
    }

    /**
     * get hosts from readHosts() because there may be multiple hosts
     * and it's configurable in elasticsearch.properties file in resources
     *
     * package private for testing
     */
    private TransportClient createClient() throws UnknownHostException {
        Settings settings = readSettings();
        TransportClient client = new PreBuiltTransportClient(settings);

        List<Host> hosts = readHosts();

        for (Host host : hosts) {
            client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(host.getHostName()), host.getPort()));
        }
        return client;
    }

    @Override
    public void run() {

        final int bulkSize = elasticsearchSetting.getBulkSize();
        final String indexName = elasticsearchSetting.getIndexName();
        List<ElasticsearchWebpageModel> models = new ArrayList<>();
        while (true) {
            models.clear();
            for (int i = 0; i < bulkSize; i++) {
                try {
                    models.add(elasticQueue.take());
                } catch (InterruptedException e) {
                    //todo
                    e.printStackTrace();
                }
            }

            try {
                submit(models, indexName);
                successfulSubmit++;
            } catch (IOException e) {
                //todo
                failureSubmit++;
                e.printStackTrace();
            }
        }

    }

    boolean submit(List<ElasticsearchWebpageModel> models, String indexName) throws IOException {
        XContentBuilder builder;
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (ElasticsearchWebpageModel model : models) {
            builder = XContentFactory.jsonBuilder();
            builder.startObject()
                    .field("url", model.getUrl())
                    .field("content", model.getArticle())
                    .field("title", model.getTitle())
                    .field("description", model.getDescription())
                    .endObject();
            bulkRequestBuilder.add(
                    client.prepareIndex(indexName, "_doc", getId(model.getUrl())).setSource(builder));
        }

        return !bulkRequestBuilder.get().hasFailures();
    }

    String getId(String url) {
        return Integer.toString(url.hashCode());
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        client.close();
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
