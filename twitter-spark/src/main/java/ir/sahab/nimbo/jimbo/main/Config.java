package ir.sahab.nimbo.jimbo.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
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
        TWITTER_ACCESS_TOKEN = props.getProperty("access_token");
        TWITTER_CONSUMER_KEY = props.getProperty("consumer_key");
        TWITTER_ACCESS_TOKEN_SECRET = props.getProperty("access_token_secret");
        TWITTER_CONSUMER_SECRET = props.getProperty("consumer_secret");
    }
}
