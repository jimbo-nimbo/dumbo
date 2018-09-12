package ir.sahab.nimbo.jimbo.elastic;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

public class ConfigSearch {



    public static final String URL_FRONTIER_TOPIC;
    public static final int BLOCKING_QUEUE_SIZE;
    public static final int CONSUMER_NUMBER;
    public static final String HBASE_TABLE_NAME;
    public static final String HBASE_DATA_CF_NAME;
    public static final String HBASE_MARK_CF_NAME;
    public static final byte[] HBASE_TABLE_NAME_BYTES;
    public static final byte[] HBASE_DATA_CF_NAME_BYTES;
    public static final byte[] HBASE_MARK_CF_NAME_BYTES;
    public static final String HBASE_SITE_DIR;
    public static final String HBASE_CORE_DIR;
    public static final int HBASE_MARK_BULK_LIMIT;
    public static final int HBASE_MARK_NUMBER_OF_THREAD;
    public static final int HBASE_DATA_BULK_LIMIT;
    public static final int HBASE_DATA_NUMBER_OF_THREAD;
    public static final int HBASE_BULK_CAPACITY;
    public static final int HBASE_COFFEIN_CACHE_TIMEOUT;
    public static final Long HBASE_DURATION_MIN;
    public static final Long HBASE_DURATION_MAX;
    public static final String LOG_PROP_DIR;
    public static final String TWITTER_CONSUMER_KEY;
    public static final String TWITTER_CONSUMER_SECRET;
    public static final String TWITTER_ACCESS_TOKEN;
    public static final String TWITTER_ACCESS_TOKEN_SECRET;

    public static final String HBASE_MARK_Q_NAME_URL;
    public static final String HBASE_MARK_Q_NAME_CONTENT_HASH;
    public static final String HBASE_MARK_Q_NAME_LAST_SEEN;
    public static final String HBASE_MARK_Q_NAME_SEEN_DURATION;
    public static final Long HBASE_MARK_DEFAULT_SEEN_DURATION;
    public static final String HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES;
    public static final byte[] HBASE_MARK_Q_NAME_URL_BYTES;
    public static final byte[] HBASE_MARK_Q_NAME_CONTENT_HASH_BYTES;
    public static final byte[] HBASE_MARK_Q_NAME_LAST_SEEN_BYTES;
    public static final byte[] HBASE_MARK_Q_NAME_SEEN_DURATION_BYTES;
    public static final byte[] HBASE_MARK_DEFAULT_SEEN_DURATION_BYTES;
    public static final byte[] HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES_BYTES;



    public static final String ES_CLUSTER_NAME;
    public static final String ES_INDEX_NAME;
    public static final String ES_SCHEME;
    public static final ArrayList<HostSearch> ES_HOSTS;
    public static final int ES_CONNECTION_TIMEOUT;
    public static final int ES_SOCKET_TIMEOUT;
    public static final int ES_MAXRETRY_TIMEOUT;
    public static final int ES_RESULT_SIZE;

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


        URL_FRONTIER_TOPIC = props.getProperty("url_frontier_topic");
        BLOCKING_QUEUE_SIZE = Integer.valueOf(props.getProperty("blocking_queue_size"));
        CONSUMER_NUMBER = Integer.valueOf(props.getProperty("consumer_number"));
        HBASE_TABLE_NAME = props.getProperty("hbase_table_name");
        HBASE_MARK_CF_NAME = props.getProperty("hbase_mark_cf_name");
        HBASE_DATA_CF_NAME = props.getProperty("hbase_data_cf_name");
        HBASE_SITE_DIR = props.getProperty("hbase_site_dir");
        HBASE_CORE_DIR = props.getProperty("hbase_core_dir");
        LOG_PROP_DIR = props.getProperty("log_prop_dir");
        HBASE_MARK_BULK_LIMIT = Integer.valueOf(props.getProperty("hbase_mark_bulk_limit"));
        HBASE_MARK_NUMBER_OF_THREAD = Integer.valueOf(props.getProperty("hbase_mark_number_of_thread"));
        HBASE_DATA_BULK_LIMIT = Integer.valueOf(props.getProperty("hbase_data_bulk_limit"));
        HBASE_DATA_NUMBER_OF_THREAD = Integer.valueOf(props.getProperty("hbase_data_number_of_thread"));
        HBASE_BULK_CAPACITY = Integer.valueOf(props.getProperty("hbase_bulk_capacity"));
        TWITTER_ACCESS_TOKEN = props.getProperty("access_token");
        TWITTER_CONSUMER_KEY = props.getProperty("consumer_key");
        TWITTER_ACCESS_TOKEN_SECRET = props.getProperty("access_token_secret");
        TWITTER_CONSUMER_SECRET = props.getProperty("consumer_secret");
        HBASE_MARK_Q_NAME_URL = props.getProperty("hbase_mark_q_name_url");
        HBASE_MARK_Q_NAME_LAST_SEEN = props.getProperty("hbase_mark_q_name_last_seen");
        HBASE_MARK_Q_NAME_SEEN_DURATION = props.getProperty("hbase_mark_q_name_seen_duration");
        HBASE_MARK_DEFAULT_SEEN_DURATION = Long.valueOf(props.getProperty("hbase_mark_default_seen_duration"));
        HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES = props.getProperty("hbase_mark_q_name_number_of_references");
        HBASE_MARK_Q_NAME_CONTENT_HASH = props.getProperty("hbase_mark_q_name_content_hash");
        HBASE_MARK_Q_NAME_URL_BYTES = Bytes.toBytes(HBASE_MARK_Q_NAME_URL);
        HBASE_MARK_Q_NAME_LAST_SEEN_BYTES = Bytes.toBytes(HBASE_MARK_Q_NAME_LAST_SEEN);
        HBASE_MARK_Q_NAME_SEEN_DURATION_BYTES = Bytes.toBytes(HBASE_MARK_Q_NAME_SEEN_DURATION);
        HBASE_MARK_DEFAULT_SEEN_DURATION_BYTES = Bytes.toBytes(HBASE_MARK_DEFAULT_SEEN_DURATION);
        HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES_BYTES = Bytes.toBytes(HBASE_MARK_Q_NAME_NUMBER_OF_REFERENCES);
        HBASE_MARK_Q_NAME_CONTENT_HASH_BYTES = Bytes.toBytes(HBASE_MARK_Q_NAME_CONTENT_HASH);
        HBASE_TABLE_NAME_BYTES = Bytes.toBytes(HBASE_TABLE_NAME);
        HBASE_MARK_CF_NAME_BYTES = Bytes.toBytes(HBASE_MARK_CF_NAME);
        HBASE_DATA_CF_NAME_BYTES = Bytes.toBytes(HBASE_DATA_CF_NAME);
        HBASE_DURATION_MIN = Long.valueOf(props.getProperty("hbase_duration_min"));
        HBASE_DURATION_MAX = Long.valueOf(props.getProperty("hbase_duration_max"));
        HBASE_COFFEIN_CACHE_TIMEOUT = Integer.valueOf(props.getProperty("hbase_coffein_cache_timeout"));

        ES_CLUSTER_NAME = props.getProperty("es.cluster.name");
        ES_SCHEME = props.getProperty("es.scheme");
        String[] tempArr = props.getProperty("es.hosts").split("#");
        for(String s : tempArr) {
            ES_HOSTS.add(new HostSearch(s));
        }
        ES_INDEX_NAME = props.getProperty("es.index.name");
        ES_CONNECTION_TIMEOUT = Integer.valueOf(props.getProperty("es.connection.timeout"));
        ES_SOCKET_TIMEOUT = Integer.valueOf(props.getProperty("es.socket.timeout"));
        ES_MAXRETRY_TIMEOUT = Integer.valueOf(props.getProperty("es.maxretry.timeout"));
        ES_RESULT_SIZE = Integer.valueOf(props.getProperty("es.result.size"));
    }

    public static int getScoreField(String field){
        return Integer.valueOf(props.getProperty(field + ".score"));
    }

}
