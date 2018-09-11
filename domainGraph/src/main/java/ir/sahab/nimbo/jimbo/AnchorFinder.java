package ir.sahab.nimbo.jimbo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class AnchorFinder implements Serializable {

    void extractAnchorsToHbase() throws InterruptedException, IOException {

        final Configuration hConf = createHbaseConfig();

        final SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final LongAccumulator successful = jsc.sc().longAccumulator();
        final LongAccumulator failure = jsc.sc().longAccumulator();

        /**
         * Read data from HBase
         */

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                jsc.newAPIHadoopRDD(
                        hConf,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        System.out.println(hbaseRDD.count());

        /**
         * Map to datas
         */

        final JavaRDD<Result> resultRDD = hbaseRDD.map(tuple -> tuple._2);

        /**
         * Extract anchors
         */

        final JavaPairRDD<String, String> oneRDD = resultRDD.flatMapToPair(result -> {
            final List<Tuple2<String, String>> list = new ArrayList<>();

            final String src = Bytes.toString(result.getValue(Config.DATA_CF_NAME.getBytes(), "url".getBytes()));
            URL srcUrl;
//            try {
                srcUrl = new URL(src);
//            } catch (MalformedURLException e) {
//
//            }
            srcUrl.toURI();
            final String srcHost = srcUrl.getHost();


            final List<Cell> cells = result.listCells();
            for (int i = 0; i < cells.size(); i += 2) {
                String des = Bytes.toString(result
                        .getValue(Config.DATA_CF_NAME.getBytes(), (i / 2 + "link").getBytes()));
                if (des == null) {
                    failure.add(1);
                    continue;
                }
                successful.add(1);
                final URL desUrl = new URL(des);
                desUrl.toURI();
                final String desHost = desUrl.getHost();

                list.add(new Tuple2<>(srcHost, desHost));
            }

            return list.iterator();
        });

        System.out.println(failure.value() + " , " + successful.value());
        System.out.println(resultRDD.count() + "---------");


        final JavaPairRDD<String, List> domain = oneRDD
                .groupByKey()
                .mapValues((org.apache.spark.api.java.function.Function<Iterable<String>, List>) input -> {
                    final List<String> hosts = new ArrayList<>();

                    input.forEach(hosts::add);

                    final List<Object> collect = hosts
                            .stream()
                            .collect(Collectors.groupingBy(Function.identity(),
                            Collectors.counting()))
                            .entrySet()
                            .stream().filter(stringLongEntry -> stringLongEntry.getValue() > 30)
                            .map((Function<Map.Entry<String, Long>, Object>) Map.Entry::getKey)
                            .collect(Collectors.toList());

                    return collect;
                });

        System.out.println(domain.count() + "---------");

        /**
         * part four check anchors
         */

        final LongAccumulator l = jsc.sc().longAccumulator();
        domain.map(new org.apache.spark.api.java.function.Function<Tuple2<String, List>, Object>() {
            @Override
            public Object call(Tuple2<String, List> list) throws Exception {

                final List list1 = list._2;

                l.add(1);
                StringBuilder sb = new StringBuilder();
                sb.append(list._1 + "," );
                for (Object o : list1) {
                    sb.append(o + ",");
                }

                return sb.toString();

            }
        }).saveAsTextFile("hdfs://hitler:9000/domains");
        System.out.println(l.value());

//        final List<Tuple2<String, String>> collect =
//        domain.mapValues((org.apache.spark.api.java.function.Function<List, String>) list -> {
//
//            StringBuilder sb = new StringBuilder();
//            for (Object o : list) {
//                sb.append(o + " - ");
//            }
//
//            return sb.toString();
//        })

    }

    static Configuration createHbaseConfig()
    {
        final Configuration hConf = HBaseConfiguration.create();

        final String hbaseSiteXmlPath = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.HBASE_SITE_XML)).getPath();
        final String coreSiteXmlPath = Objects.requireNonNull(AnchorFinder.class
                .getClassLoader().getResource(Config.CORE_SITE_XML)).getPath();

        hConf.addResource(new Path(hbaseSiteXmlPath));
        hConf.addResource(new Path(coreSiteXmlPath));

        hConf.set(TableInputFormat.INPUT_TABLE, Config.HBASE_TABLE);
        hConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, Config.DATA_CF_NAME);

        return hConf;
    }

}
