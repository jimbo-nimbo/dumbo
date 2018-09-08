package ir.sahab.nimbo.jimbo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.launcher.SparkLauncher;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Main {

    public static void wordCount() {
        SparkConf conf = new SparkConf()
                .setAppName("Work Count App");
//                .setMaster("spark://hitler:7077")
//                .set("spark.driver.host", "hitler");



        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("hdfs://hitler:9000/testSarb/luremIpsum.txt");
//        hdfs://hitler:9000/testSarb/luremIpsum.txt

        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator());

        JavaPairRDD<String, Integer> counts =
                words.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        counts.saveAsTextFile("hdfs://hitler:9000/b");
//        hdfs://hitler:9000/output
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        (new AnchorFinder()).extractAnchorsToHbase();

//        tt();
        Configuration hConf = HBaseConfiguration.create();
        String path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);
        System.out.println(Config.HBASE_TABLE + ", " + Config.DATA_CF_NAME);

        SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc
                .newAPIHadoopRDD(hConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        System.out.println(hbaseRDD.count());
//        Process launcher = new SparkLauncher().setAppName("testLauncher")
//                .setAppResource("spark-job.jar")
//                .setSparkHome("spark-home")
//                .setMainClass("main-class")
//                .setVerbose(true).launch();
//
//        Process spark = new SparkLauncher()
//                .setAppResource("/home/sarb/dumbo/anchor/")
//                .setMainClass("my.spark.app.Main")
//                .setMaster("local")
//                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
//                .launch();
//        spark.waitFor();
//
//        wordCount();

//        test();
        /*
        final List<Integer> test = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < i; j++) {
                test.add(j);
            }
        }

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < i; j++) {
                test.add(j);
            }
        }

        for (int i = 0; i < 1000; i++) {
            test.add(1000);
        }

//        final List<Map.Entry<Integer, Long>> collect =
        final List<Object> collect = test.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(new Comparator<Map.Entry<Integer, Long>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Long> o1, Map.Entry<Integer, Long> o2) {
                        return (int) (o2.getValue() - o1.getValue());
                    }
                })
                .limit(10)
                .map(new Function<Map.Entry<Integer, Long>, Object>() {
                    @Override
                    public Object apply(Map.Entry<Integer, Long> integerLongEntry) {
                        return integerLongEntry.getKey();
                    }
                })
                .collect(Collectors.toList());

        collect.forEach(System.out::println);
        */

    }

    private static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("hitler", 9200, "http")));

    private static void test() throws InterruptedException, IOException {
        final SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        final JavaRDD<Integer> parallelize = jsc.parallelize(list);

        parallelize.foreach((VoidFunction<Integer>) integer -> {
            //Create Request
            final UpdateRequest request = new UpdateRequest(
                    "posts",
                    "doc",
                    "www.test" + integer.toString() + ".com");

            //Create document with ContentBuilder
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("test", integer.toString());
            }
            builder.endObject();

            //bind document to request
            request.doc(builder);

            client.updateAsync(request, new ActionListener<UpdateResponse>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    System.out.println("good!" + integer.toString());
                }

                @Override
                public void onFailure(Exception e) {
                    System.out.println("bad!" + integer.toString());
                }
            });
        });

        Thread.sleep(10000);
        client.close();
    }

    private static void tt() throws IOException {
        Configuration hConf = HBaseConfiguration.create();
        String path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        path = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);

        SparkConf sparkConf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> countRDD = jsc.parallelize(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
                .mapToPair((PairFunction<Integer, String, Integer>) integer ->
                        new Tuple2<>(integer.toString(), integer));

        Job job = Job.getInstance(hConf);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Config.HBASE_TABLE);
        job.setOutputFormatClass(TableOutputFormat.class);
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = countRDD.mapToPair(row -> {
            String rowKey = row._1;
            Put put = new Put(DigestUtils.md5Hex(rowKey).getBytes());
            put.addColumn(Bytes.toBytes(Config.MARK_CF_NAME), Bytes.toBytes("Refers"), Bytes.toBytes(row._2));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }
}
