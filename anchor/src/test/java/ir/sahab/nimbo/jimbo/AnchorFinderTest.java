package ir.sahab.nimbo.jimbo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class AnchorFinderTest
{
    private static JavaSparkContext jsc;

    @BeforeClass
    public static void createSparkContext() {
        final SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME).setMaster("spark://hitler:7077");
        jsc = new JavaSparkContext(conf);
    }

    @Test
    public void testSpark() {

        final JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        final List<Integer> collect = parallelize.collect();

        for (int i = 0; i < collect.size(); i++) {
            assertEquals(i, collect.get(i).intValue());
        }
    }

    @Test
    public void testHbase() {
        // define Spark Context
//        final SQLContext sqlContext = new SQLContext(jsc);

        // create connection with HBase
        Configuration config = null;
        try {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "hitler");
            config.set("hbase.zookeeper.property.clientPort","2181");
            //config.set("hbase.master", "127.0.0.1:60000");
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is running!");
        }
        catch (MasterNotRunningException e) {
            System.out.println("HBase is not running!");
            System.exit(1);
        }catch (Exception ce){
            ce.printStackTrace();
        }

        config.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME); // column family
//        config.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs"); // 3 column qualifiers
    }

}