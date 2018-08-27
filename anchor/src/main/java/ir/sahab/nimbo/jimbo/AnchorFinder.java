package ir.sahab.nimbo.jimbo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AnchorFinder {

    public static void findAnchors() {

        final SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final Configuration hConf = HBaseConfiguration.create();
        final String hbaseSiteXml = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        final String coreSiteXml = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();

        hConf.addResource(new Path(hbaseSiteXml));
        hConf.addResource(new Path(coreSiteXml));

        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.HBASE_TABLE);

        final JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc
                .newAPIHadoopRDD(hConf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        final JavaRDD<Result> map = hbaseRDD.map(tuple -> tuple._2);

        final JavaPairRDD<String, String> stringStringJavaPairRDD = map.flatMapToPair(result -> {
            List<Tuple2<String, String>> list = new ArrayList<>();
            List<Cell> cells = result.listCells();
            for (int i = 0; i < cells.size(); i += 2) {
                String anchor = Bytes.toString(CellUtil.cloneValue(cells.get(i)));
                String link = Bytes.toString(CellUtil.cloneValue(cells.get(i + 1)));
                list.add(new Tuple2<>(link, anchor));
            }
            return list.iterator();
        });

        JavaPairRDD<String, String> stringStringJavaPairRDD1 =
                stringStringJavaPairRDD.reduceByKey((val1, val2) -> val1 + "#" + val2);

        List<Tuple2<String, String>> collect =
                stringStringJavaPairRDD1.sample(false, 0.000001, 0).collect();

        for (Tuple2<String, String> stringStringTuple2 : collect) {
            System.out.println(stringStringTuple2._1 + ": " + stringStringTuple2._2);
        }

    }

    public static void main(String[] args) {
        findAnchors();
    }
}
