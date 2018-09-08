package ir.sahab.nimbo.jimbo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class AnchorFinderTest
{
    private static JavaSparkContext jsc;
    private static int testGraphSize = 10;
    private static String testQualifier = "test";
    private static String testColumnFamily = "test";

    @BeforeClass
    public static void createSparkContext() {
        final SparkConf conf = new SparkConf()
                .setAppName(Config.SPARK_APP_NAME)
                .setMaster("local");

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

//    @Test
    public void createGraph() throws IOException {

        final Configuration config = AnchorFinder.createHbaseConfiguration();

        config.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);

        final Job newAPIJobConfiguration = Job.getInstance(config);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Config.HBASE_TABLE);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        final List<Integer> vertices = new ArrayList<>();
        for (int i = 0; i < testGraphSize; i++) {
            vertices.add(i);
        }

        final JavaRDD<Integer> verticesRdd = jsc.parallelize(vertices);
        final JavaPairRDD<String, List<String>> anchorsRdd =
                verticesRdd.mapToPair((PairFunction<Integer, String, List<String>>) integer -> {

            final List<String> anchors = new ArrayList<>();
            int src = integer.intValue();
            for (int i = 1; i < testGraphSize; i++) {
                final int des = (i + src) % testGraphSize;
                for (int j = 0; j < testGraphSize - i; j++) {
                    anchors.add("anchor from " + src + " to " + des);
                    anchors.add("www.test" + des + ".com");
                }
            }

            final String srcLink = "www.test" + integer + ".com";
            return new Tuple2<>(srcLink, anchors);
        });

        final JavaPairRDD<ImmutableBytesWritable, Put> hbasePut = anchorsRdd.mapToPair(
                (PairFunction<Tuple2<String, List<String>>, ImmutableBytesWritable, Put>) stringListTuple2 -> {
                    final String src = stringListTuple2._1;
                    final List<String> anchors = stringListTuple2._2;

                    final Put put = new Put(DigestUtils.md5Hex(src).getBytes());
                    for (int i = 0; i < anchors.size(); i+=2) {
                        put.addColumn(Config.DATA_CF_NAME.getBytes(), ("anchor" + i).getBytes(), anchors.get(i).getBytes());
                        put.addColumn(Config.DATA_CF_NAME.getBytes(), ("link" + i).getBytes(), anchors.get(i + 1).getBytes());
                    }

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });

//         create Key, Value pair to store in HBase
//        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = anchorsRdd.mapToPair(
//                new PairFunction<Row, ImmutableBytesWritable, Put>() {
//                    @Override
//                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
//
//                        Put put = new Put(Bytes.toBytes(row.getString(0)));
//                        put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row.getString(1)));
//                        put.add(Bytes.toBytes("columFamily"), Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row.getString(2)));
//
//                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
//                    }
//                });

        // save to HBase- Spark built-in API method
        hbasePut.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
    }

//    @Test
    public void pureTest() throws IOException, InterruptedException {
        // create connection with HBase
        /*
        Configuration config = null;
        try {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "127.0.0.1");
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

        config.set(TableInputFormat.INPUT_TABLE, "testtest");

        *//////////////////////////////////////////////////////////////

        Configuration hConf = HBaseConfiguration.create();
        String path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);

        Job newAPIJobConfiguration1 = Job.getInstance(hConf);
        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "testtest");
        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);


        List<Integer> lists = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            lists.add(i);
        }

        final JavaRDD<Integer> parallelize = jsc.parallelize(lists);

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = parallelize.mapToPair(
                (PairFunction<Integer, ImmutableBytesWritable, Put>) row -> {

                    Put put = new Put(Bytes.toBytes(row));
                    put.addColumn(Bytes.toBytes("cf"),
                            Bytes.toBytes("columnQualifier1"), Bytes.toBytes(row + 1));
                    put.addColumn(Bytes.toBytes("cf"),
                            Bytes.toBytes("columnQualifier2"), Bytes.toBytes(row));

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });

        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
    }

    @Test
    public void connTest() throws IOException, InterruptedException {
        final Configuration hbaseConfig = AnchorFinder.createHbaseConfiguration();

        final Job job = Job.getInstance(hbaseConfig);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Config.HBASE_TABLE);
        job.setOutputFormatClass(TableOutputFormat.class);
        final JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = jsc
                .parallelize(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
                .mapToPair((PairFunction<Integer, String, Integer>) integer ->
                        new Tuple2<>(integer.toString(), integer))
                .mapToPair(row -> {
                    String rowKey = row._1;
                    Put put = new Put(DigestUtils.md5Hex(rowKey).getBytes());
                    put.addColumn(Bytes.toBytes(testColumnFamily),
                            Bytes.toBytes(testQualifier),
                            Bytes.toBytes(row._2));
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });
        hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());

        Thread.sleep(10000);

        final JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                jsc.newAPIHadoopRDD(
                        createHbaseConfig(),
                        TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        System.out.println(hbaseRDD.count());

        final JavaRDD<Integer> resultRDD = hbaseRDD.map(tuple -> {
            final Cell cell = tuple._2.listCells().get(0);
            return Bytes.toInt(CellUtil.cloneValue(cell));
        });

        final List<Integer> collect = resultRDD.collect();
        System.out.println(collect.size());
        for (int i = 0; i < collect.size(); i++) {
            System.out.println(i);
            assertEquals(i, collect.get(i).intValue());
        }

    }

    Configuration createHbaseConfig()
    {
        final Configuration hConf = HBaseConfiguration.create();

        final String hbaseSiteXmlPath = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        final String coreSiteXmlPath = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();

        hConf.addResource(new Path(hbaseSiteXmlPath));
        hConf.addResource(new Path(coreSiteXmlPath));

        System.out.println(Config.HBASE_TABLE + "<---");

        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);

        return hConf;
    }

    @Test
    public void test() {
        Configuration hConf = HBaseConfiguration.create();
        String path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);

        System.out.println(Config.HBASE_TABLE + ", " + Config.MARK_CF_NAME);

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc
                .newAPIHadoopRDD(hConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        System.out.println(hbaseRDD.count());
    }

}