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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class AnchorFinderTest {
    private static JavaSparkContext jsc;
    private static int testGraphSize = 50;
    private static String testQualifier = "test";

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

    @Test
    public void createGraph() throws IOException {

        final Configuration hbaseConfig = AnchorFinder.createHbaseConfig();

        final Job job = Job.getInstance(hbaseConfig);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Config.HBASE_TABLE);
        job.setOutputFormatClass(TableOutputFormat.class);

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
                    for (int i = 0; i < anchors.size(); i += 2) {
                        put.addColumn(Config.DATA_CF_NAME.getBytes(), ("anchor" + i/2).getBytes(), anchors.get(i).getBytes());
                        put.addColumn(Config.DATA_CF_NAME.getBytes(), ("link" + i/2).getBytes(), anchors.get(i + 1).getBytes());
                    }

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });
        final List<String> collect = hbasePut.map(t -> t._2)
                .map((Function<Put, String>) put -> put.getId()).sample(false, 0.02)
                .collect();
        for (String o : collect) {
            System.out.println(o);
        }

        hbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    @Test
    public void testHbase() throws IOException, InterruptedException {
        final Configuration hbaseConfig = AnchorFinder.createHbaseConfig();

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
                    put.addColumn(Bytes.toBytes(Config.DATA_CF_NAME),
                            Bytes.toBytes(testQualifier),
                            Bytes.toBytes(row._2));
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                });
        hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());

        Thread.sleep(1000);

        final JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                jsc.newAPIHadoopRDD(
                        AnchorFinder.createHbaseConfig(),
                        TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);

        final JavaRDD<Integer> resultRDD = hbaseRDD.map(tuple -> {
            final Cell cell = tuple._2.listCells().get(0);
            return Bytes.toInt(CellUtil.cloneValue(cell));
        });


        final List<Integer> collect = resultRDD
                .collect();
        assertEquals(10, collect.size());
    }

    @Test
    public void readHbase() {
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

        final List<Tuple2<String, String>> collect = hbaseRDD.map(tuple -> tuple._2)
                .flatMapToPair((PairFlatMapFunction<Result, String, String>) result -> {
                    final List<Tuple2<String, String>> list = new ArrayList<>();

                    final List<Cell> cells = result.listCells();
                    for (int i = 0; i < cells.size(); i += 2) {
                        String anchor = Bytes.toString(result
                                        .getValue(Config.DATA_CF_NAME.getBytes(), ("anchor" + i / 2).getBytes()));
                        String link = Bytes.toString(result
                                        .getValue(Config.DATA_CF_NAME.getBytes(), ("link" + i / 2).getBytes()));

                        list.add(new Tuple2<>(link, anchor));
                    }

                    return list.iterator();
                }).collect();

        for (int i = 0; i < collect.size(); i++) {
            System.out.println(i + " : " + collect.get(i));
        }

    }

}