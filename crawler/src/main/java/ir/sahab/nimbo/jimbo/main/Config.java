package ir.sahab.nimbo.jimbo.main;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static final Logger logger = LoggerFactory.getLogger(Config.class);

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


    public static final String METRICS_DIR;

    static {
        String resourceName = "conf.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
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
        METRICS_DIR = props.getProperty("metrics_dir");
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
    }
}
