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
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NumRefers {

    static void extractNumRefers() throws IOException {
        Configuration hConf = HBaseConfiguration.create();
        String path = Objects.requireNonNull(NumRefers.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        path = Objects.requireNonNull(NumRefers.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();
        hConf.addResource(new Path(path));
        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);

        SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc
                .newAPIHadoopRDD(hConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        JavaRDD<Result> resultRDD = hbaseRDD.map(tuple -> tuple._2);
        JavaPairRDD<String, Integer> oneRDD = resultRDD.flatMapToPair(result -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (Cell cell : result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (qualifier.endsWith("link"))
                    list.add(new Tuple2<>(Bytes.toString(CellUtil.cloneValue(cell)), 1));
            }
            return list.iterator();
        });
        JavaPairRDD<String, Integer> countRDD = oneRDD.reduceByKey((v1, v2) -> v1 + v2);

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
