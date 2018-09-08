package ir.sahab.nimbo.jimbo.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

abstract public class AbstractHBase {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHBase.class);

    protected TableName tableName;
    Table table = null;

    protected AbstractHBase(String tableName) {
        this.tableName = TableName.valueOf(tableName);
        Connection connection = HBaseConnection.getInstance().getConnection();
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
