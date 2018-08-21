package ir.sahab.nimbo.jimbo.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Setting {

    private static final Logger logger = LoggerFactory.getLogger(Setting.class);

    protected final Properties properties;

    protected Setting(String propertiesFileName) {
        String resourceName = propertiesFileName + ".properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            properties.load(resourceStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
