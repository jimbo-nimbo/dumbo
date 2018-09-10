package ir.sahab.nimbo.jimbo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    public static final String HBASE_INPUT_TABLE;
    public static final String HBASE_OUTPUT_TABLE;
    public static final String META_CF_NAME;
    public static final Integer BULK_SIZE;
    public static final String URL_COL_NAME;

    static {
        String resourceName = "conf.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        HBASE_INPUT_TABLE = props.getProperty("hbase_input_table");
        HBASE_OUTPUT_TABLE = props.getProperty("hbase_output_table");
        META_CF_NAME = props.getProperty("hbase_meta_cf_name");
        URL_COL_NAME = props.getProperty("hbase_url_col_name");
        BULK_SIZE = Integer.valueOf(props.getProperty("bulk_size"));
    }
}
