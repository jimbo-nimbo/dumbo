package ir.sahab.nimbo.jimbo;

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
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NumRefers {

    static String reverseUrl(URL url) {
        // TODO: some strings will exceed the limit (3000 bytes)
        return reverseDomain(url.getHost()) + url.getPath();
    }

    static String reverseDomain(String domain) {
        StringBuilder stringBuilder = new StringBuilder();
        String[] res = domain.split("\\.");
        try {
            stringBuilder.append(res[res.length - 1]);
            for (int i = 1; i < res.length; i++) {
                stringBuilder.append(".").append(res[res.length - 1 - i]);
            }
        } catch (IndexOutOfBoundsException e1) {
            // TODO: log
            //Logger.getInstance().debugLog(e1.getMessage());
        }
        // TODO: some string builders are empty
        if (stringBuilder.toString().equals(""))
            return "bad.site";
        return stringBuilder.toString();
    }

    public static void extractNumRefers() throws IOException {
        SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Configuration hConf = HBaseConfiguration.create();
        String path = Objects.requireNonNull(NumRefers.class
                .getClassLoader().getResource(Config.SPARK_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        path = Objects.requireNonNull(NumRefers.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);


        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc
                .newAPIHadoopRDD(hConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        JavaRDD<Result> resultRDD = hbaseRDD.map(tuple -> tuple._2);
        JavaPairRDD<String, String> keyValueRDD = resultRDD.flatMapToPair(result -> {
            List<Tuple2<String, String>> list = new ArrayList<>();
            for (Cell cell : result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (qualifier.endsWith("link"))
                    list.add(new Tuple2<>(Bytes.toString(result.getRow()).split(" ")[0],
                            Bytes.toString(CellUtil.cloneValue(cell))));
            }
            return list.iterator();
        });

        JavaPairRDD<String, Integer> oneRDD = keyValueRDD.mapToPair(pair -> new Tuple2<>(pair._2, 1));
        JavaPairRDD<String, Integer> countRDD = oneRDD.reduceByKey((v1, v2) -> v1 + v2);

        Job job = Job.getInstance(hConf);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Config.HBASE_TABLE);
        job.setOutputFormatClass(TableOutputFormat.class);
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = countRDD.mapToPair(row -> {
            String rowKey = row._1;
            if (rowKey.length() == 0)
                rowKey = "http://bad.site/";
            Put put = new Put(reverseUrl(new URL(rowKey)).getBytes());
            //Put put = new Put(Bytes.toBytes(row._1));
            put.addColumn(Bytes.toBytes(Config.MARK_CF_NAME), Bytes.toBytes("Ref"), Bytes.toBytes(row._2));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }
}
