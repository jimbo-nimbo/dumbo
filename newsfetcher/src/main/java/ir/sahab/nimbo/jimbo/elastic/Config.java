package ir.sahab.nimbo.jimbo.elastic;

import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Config {
    public static final String ES_CLUSTER_NAME;
    public static final String ES_INDEX_NAME;
    public static final String ES_SCHEME;
    public static final ArrayList<Host> ES_HOSTS;
    public static final int ES_CONNECTION_TIMEOUT;
    public static final int ES_SOCKET_TIMEOUT;
    public static final int ES_MAXRETRY_TIMEOUT;
    private static Properties props;
    static {
        ES_HOSTS = new ArrayList<>();
        String resourceName = "conf.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ES_CLUSTER_NAME = props.getProperty("es.cluster.name");
        ES_SCHEME = props.getProperty("es.scheme");
        String[] tempArr = props.getProperty("es.hosts").split("#");
        for(String s : tempArr) {
            ES_HOSTS.add(new Host(s));
        }
        ES_INDEX_NAME = props.getProperty("es.index.name");
        ES_CONNECTION_TIMEOUT = Integer.valueOf(props.getProperty("es.connection.timeout"));
        ES_SOCKET_TIMEOUT = Integer.valueOf(props.getProperty("es.socket.timeout"));
        ES_MAXRETRY_TIMEOUT = Integer.valueOf(props.getProperty("es.maxretry.timeout"));

    }

    public static int getScoreField(String field){
        return Integer.valueOf(props.getProperty(field + ".score"));
    }

}
