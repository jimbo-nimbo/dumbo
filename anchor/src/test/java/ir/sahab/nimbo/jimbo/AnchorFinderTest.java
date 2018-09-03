package ir.sahab.nimbo.jimbo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

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

//        config.set(TableInputFormat.INPUT_TABLE, "tableName");

        config.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME); // column family
        config.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs"); // 3 column qualifiers

        final JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, TestData> rowPairRDD = hBaseRDD.mapToPair(
                (PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, TestData>) entry -> {

                    Result r = entry._2;
                    String keyRow = Bytes.toString(r.getRow());

                    TestData cd = new TestData();
                    cd.setRowkey(keyRow);
                    cd.setVc(Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("vc"))));
                    cd.setVs(Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("vs"))));
                    return new Tuple2<>(keyRow, cd);
                });

        Map<String, TestData> stringTestDataMap = rowPairRDD.collectAsMap();
        stringTestDataMap.forEach((s, testData) ->
                System.out.println(
                        s + " -> " + testData.getRowkey() + ", vc" + testData.getVc() + ", vs" + testData.getVs()));

    }

    class TestData implements Serializable {
        private String rowkey;
        private String vc;
        private String vs;

        public void setRowkey(String rowkey)
        {
            this.rowkey = rowkey;
        }

        public void setVc(String vc)
        {
            this.vc = vc;
        }

        public void setVs(String vs)
        {
            this.vs = vs;
        }

        public String getRowkey()
        {
            return rowkey;
        }

        public String getVc()
        {
            return vc;
        }

        public String getVs()
        {
            return vs;
        }
    }

}