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

    public static final String HBASE_TABLE_NAME;
    public static final String HBASE_DATA_CF_NAME;
    public static final String HBASE_MARK_CF_NAME;
    public static final String HBASE_SITE_DIR;
    public static final String HBASE_CORE_DIR;
    public static final int HBASE_MIN_THREAD;
    public static final int HBASE_MAX_THREAD;
    public static final int HBASE_EXECUROR_BLOCK_Q_SIZE;
    public static final int HBASE_BULK_LIMIT;
    public static final int HBASE_BULK_CAPACITY;
    public static final int HBASE_BULK_THREAD_SIZE;
    public static final String HBASE_MARK_Q_NAME_URL;
    public static final String HBASE_MARK_Q_NAME_LAST_SEEN;
    public static final String HBASE_MARK_Q_NAME_SEEN_DURATION;
    public static final String HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES;


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

        HBASE_TABLE_NAME = props.getProperty("hbase_table_name");
        HBASE_MARK_CF_NAME = props.getProperty("hbase_mark_cf_name");
        HBASE_DATA_CF_NAME = props.getProperty("hbase_data_cf_name");
        HBASE_SITE_DIR = props.getProperty("hbase_site_dir");
        HBASE_CORE_DIR = props.getProperty("hbase_core_dir");
        HBASE_MIN_THREAD = Integer.valueOf(props.getProperty("hbase_min_thread"));
        HBASE_MAX_THREAD = Integer.valueOf(props.getProperty("hbase_max_thread"));
        HBASE_EXECUROR_BLOCK_Q_SIZE = Integer.valueOf(props.getProperty("executor_service_block_q_size"));
        HBASE_BULK_LIMIT = Integer.valueOf(props.getProperty("hbase_bulk_limit"));
        HBASE_BULK_CAPACITY = Integer.valueOf(props.getProperty("hbase_bulk_capacit"));
        HBASE_BULK_THREAD_SIZE = Integer.valueOf(props.getProperty("hbase_balk_thread_size"));
        HBASE_MARK_Q_NAME_URL = props.getProperty("hbase_mark_q_name_url");
        HBASE_MARK_Q_NAME_LAST_SEEN = props.getProperty("hbase_mark_q_name_last_seen");
        HBASE_MARK_Q_NAME_SEEN_DURATION = props.getProperty("hbase_mark_q_name_seen_duration");
        HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES = props.getProperty("hbase_mark_q_name_number_of_references");
    }

    public static int getScoreField(String field){
        return Integer.valueOf(props.getProperty(field + ".score"));
    }

}
