package ir.sahab.nimbo.jimbo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class HBaseConnection {
    private static final Logger logger = LoggerFactory.getLogger(HBaseConnection.class);

    private static HBaseConnection ourInstance = null;

    private Connection connection = null;

    public static HBaseConnection getInstance() {
        if (ourInstance == null)
            ourInstance = new HBaseConnection();
        return ourInstance;
    }

    private HBaseConnection() {
        Configuration config = HBaseConfiguration.create();
        String path = Objects.requireNonNull(this.getClass().getClassLoader()
                .getResource("hbase-site.xml")).getPath();
        config.addResource(new Path(path));
        path = Objects.requireNonNull(this.getClass().getClassLoader()
                .getResource("core-site.xml")).getPath();
        config.addResource(new Path(path));
        boolean conn = true;
        while (conn) {
            try {
                connection = ConnectionFactory.createConnection(config);
                conn = false;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }
}
