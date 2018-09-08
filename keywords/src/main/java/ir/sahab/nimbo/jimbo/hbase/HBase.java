package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.Config;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HBase extends AbstractHBase {

    private static HBase hbase = new HBase();

    private HBase() {
        super(Config.HBASE_INPUT_TABLE);
    }

    @Override
    protected void initializeTable(Connection connection) throws IOException {
    }
}