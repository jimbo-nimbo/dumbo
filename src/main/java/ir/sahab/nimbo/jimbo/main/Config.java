package ir.sahab.nimbo.jimbo.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    public static final String URL_FRONTIER_TOPIC;
    public static final int FETCHER_THREAD_NUM;
    public static final int PARSER_THREAD_NUM;
    public static final int BLOCKING_QUEUE_SIZE;
    public static final int CONSUMER_NUMBER;
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
    public static final String LOG_PROP_DIR;
    public static final String TWITTER_CONSUMER_KEY;
    public static final String TWITTER_CONSUMER_SECRET;
    public static final String TWITTER_ACCESS_TOKEN;
    public static final String TWITTER_ACCESS_TOKEN_SECRET;



    static {
        String resourceName = "conf.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        URL_FRONTIER_TOPIC = props.getProperty("url_frontier_topic");
        FETCHER_THREAD_NUM = Integer.valueOf(props.getProperty("fetcher_thread_num"));
        PARSER_THREAD_NUM = Integer.valueOf(props.getProperty("parser_thread_num"));
        BLOCKING_QUEUE_SIZE = Integer.valueOf(props.getProperty("blocking_queue_size"));
        CONSUMER_NUMBER = Integer.valueOf(props.getProperty("consumer_number"));
        HBASE_TABLE_NAME = props.getProperty("hbase_table_name");
        HBASE_MARK_CF_NAME = props.getProperty("hbase_mark_cf_name");
        HBASE_DATA_CF_NAME = props.getProperty("hbase_data_cf_name");
        HBASE_SITE_DIR = props.getProperty("hbase_site_dir");
        HBASE_CORE_DIR = props.getProperty("hbase_core_dir");
        HBASE_MIN_THREAD = Integer.valueOf(props.getProperty("hbase_min_thread"));
        HBASE_MAX_THREAD = Integer.valueOf(props.getProperty("hbase_max_thread"));
        HBASE_EXECUROR_BLOCK_Q_SIZE = Integer.valueOf(props.getProperty("executor_service_block_q_size"));
        LOG_PROP_DIR = props.getProperty("log_prop_dir");
        HBASE_BULK_LIMIT = Integer.valueOf(props.getProperty("hbase_bulk_limit"));
        HBASE_BULK_CAPACITY = Integer.valueOf(props.getProperty("hbase_bulk_capacit"));
        HBASE_BULK_THREAD_SIZE = Integer.valueOf(props.getProperty("hbase_balk_thread_size"));
        TWITTER_ACCESS_TOKEN = props.getProperty("access_token");
        TWITTER_CONSUMER_KEY = props.getProperty("consumer_key");
        TWITTER_ACCESS_TOKEN_SECRET = props.getProperty("access_token_secret");
        TWITTER_CONSUMER_SECRET = props.getProperty("consumer_secret");
    }
}
