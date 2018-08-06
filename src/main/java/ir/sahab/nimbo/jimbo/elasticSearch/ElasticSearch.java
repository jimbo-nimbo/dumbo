package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.main.Logger;
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

public class ElasticSearch {
    private TransportClient client;

    public ElasticSearch() throws ElasticCannotLoadException {
        try {
            Properties properties = getProperties();
            client = createClient(properties);
        } catch (IOException e) {
            Logger.getInstance().logToFile( "Error in Elasticsearch constructor "
                    + e.getMessage());
            throw new ElasticCannotLoadException();
        }
    }

    /**
     * create it as an separate function for testing
     *
     * package private for testing
     */
    Properties getProperties() throws IOException {
        String resourceName = "elasticsearch.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(resourceName);

        Properties props = new Properties();
        props.load(resourceStream);

        return props;
    }

    /**
     * get hosts from getHosts() because there may be multiple hosts
     * and it's configurable in elasticsearch.properties file in resources
     *
     * package private for testing
     */
    TransportClient createClient(Properties properties) throws UnknownHostException {
        Settings settings = getSettings(properties);
        TransportClient client = new PreBuiltTransportClient(settings);

        List<Host> hosts = getHosts(properties);
        for (Host host : hosts) {
            client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(host.getHostName()), host.getPort()));
        }
        return client;
    }

    /**
     * extract settings from properties object
     * read name from properties file
     *
     * set client.transport.sniff true for sending requests to master node //TODO: check this configuration
     * @see <a href="https://www.elastic.co/guide/en/
     *      elasticsearch/client/java-api/current/transport-client.html#transport-client">
     *      elasticsearch transport client</a>
     *
     * package private for testing
     */
    Settings getSettings(Properties properties) {
        String clusterName = properties.getProperty("cluster.name");
        return Settings.builder()
                .put("cluster.name", clusterName)
//                .put("client.transport.sniff", true)
                .build();
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
    List<Host> getHosts(Properties properties) {
        List<Host> hosts = new ArrayList<>();
        String hostsString = properties.getProperty("hosts");
        for (String hostString : hostsString.split("#")) {
            Host host = new Host(hostString.split(":")[0],
                    Integer.parseInt(hostsString.split(":")[1]));
            hosts.add(host);
        }
        return hosts;
    }

    /**
     * close client connection at the end of program
     * because we have only this connection so far
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        client.close();
    }

    /**
     * class for encapsulate hosts
     */
    private class Host {
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
