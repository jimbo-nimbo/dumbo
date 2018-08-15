package ir.sahab.nimbo.jimbo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    public static final String SPARK_APP_NAME;
    public static final String SPARK_SITE_XML;
    public static final String CORE_SITE_XML;
    public static final String HBASE_TABLE;
    public static final String MARK_CF_NAME;

    static {
        String resourceName = "conf.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SPARK_APP_NAME = props.getProperty("spark_app_name");
        SPARK_SITE_XML = props.getProperty("hbase_site_xml");
        CORE_SITE_XML = props.getProperty("core_site_xml");
        HBASE_TABLE = props.getProperty("hbase_table");
        MARK_CF_NAME = props.getProperty("hbase_mark_cf_name");
    }
}
