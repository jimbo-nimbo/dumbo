package ir.sahab.nimbo.jimbo;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ElasticClientBuilder {
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
    private static List<Host> readHosts() {
        List<Host> hosts = new ArrayList<>();
        String hostsString = ElasticConfig.HOSTS;
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
    private static Settings readSettings() {
        return Settings.builder()
                .put("cluster.name", ElasticConfig.CLUSTER_NAME)
                .put("client.transport.sniff", true)
                .build();
    }

    /**
     * get hosts from readHosts() because there may be multiple hosts
     * and it's configurable in elasticsearch.properties file in resources
     */
    public static TransportClient build() throws UnknownHostException {
        Settings settings = readSettings();
        TransportClient client = new PreBuiltTransportClient(settings);

        List<Host> hosts = readHosts();

        for (Host host : hosts) {
            client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(host.getHostName()), host.getPort()));
        }
        return client;
    }

    /**
     * class to encapsulate hosts
     */
    static class Host {
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
