package ir.sahab.nimbo.jimbo.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

abstract public class AbstractHBase {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHBase.class);

    protected TableName tableName;
    Table table = null;

    protected AbstractHBase(String tableName) {
        this.tableName = TableName.valueOf(tableName);
        Configuration config = HBaseConfiguration.create();
        String path = Objects.requireNonNull(this.getClass().getClassLoader()
                .getResource("hbase-site.xml")).getPath();
        config.addResource(new Path(path));
        path = Objects.requireNonNull(this.getClass().getClassLoader()
                .getResource("core-site.xml")).getPath();
        config.addResource(new Path(path));
        boolean conn = true;
        Connection connection = null;
        while (conn) {
            try {
                connection = ConnectionFactory.createConnection(config);
                conn = false;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        try {
            initializeTable(connection);
            table = connection.getTable(this.tableName);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    abstract protected void initializeTable(Connection connection) throws IOException;

    protected String makeRowKey(String row){
        return DigestUtils.md5Hex(row);
    }
}
