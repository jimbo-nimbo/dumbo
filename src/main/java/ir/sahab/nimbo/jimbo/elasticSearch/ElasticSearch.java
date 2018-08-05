package ir.sahab.nimbo.jimbo.elasticSearch;

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

    public ElasticSearch() throws IOException {
        Properties properties = getProperties();
        client = createClient(properties);
    }

    /**
     * get properties file of elasticsearch
     *
     * @return property file of elasticsearch (stored in resources)
     * @throws IOException if cannot load the elasticsearch.properties
     * @author Alireza
     */
    private static Properties getProperties() throws IOException {
        String resourceName = "elasticsearch.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        InputStream resourceStream = loader.getResourceAsStream(resourceName);
        props.load(resourceStream);
        return props;
    }

    /**
     * build client
     *
     * @param properties properties object
     * @throws UnknownHostException //TODO: handle exception
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
     * set client.transport.sniff true for sending requests to master node
     *
     * @param properties properties object
     * @return settings for client
     */
    private Settings getSettings(Properties properties) {
        String clusterName = properties.getProperty("cluster.name");
        return Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();
    }

    /**
     * get hosts from .properties file
     * hosts are separated with #
     * example:
     * host1:port#host2:port
     *
     * @param properties properties object
     * @return list of hosts
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
     * close client connection
     *
     * @throws Throwable default finalize signature
     * @author Alireza
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
