package ir.sahab.nimbo.jimbo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AnchorFinder implements Serializable {

    private static final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("genghis", 9200, "http"),
                    new HttpHost("hitler", 9200, "http")));



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

            final List<Cell> cells = result.listCells();
            for (int i = 0; i < cells.size(); i += 2) {
                String anchor = Bytes.toString(result
                        .getValue(Config.DATA_CF_NAME.getBytes(), (i / 2 + "anchor").getBytes()));
                String link = Bytes.toString(result
                        .getValue(Config.DATA_CF_NAME.getBytes(), (i / 2 + "link").getBytes()));
                if (anchor == null) {
                    failure.reset();
                    failure.add(i/2 + 1);
                    continue;
                }

                final String[] split = anchor.split("\\s+");
                for (String s : split) {
                    list.add(new Tuple2<>(link, s));
                }
            }

            return list.iterator();
        });

        final JavaPairRDD<String, List> stringObjectJavaPairRDD = oneRDD
                .groupByKey()
                .mapValues((org.apache.spark.api.java.function.Function<Iterable<String>, List>) input -> {
                    final List<String> anchors = new ArrayList<>();
                    input.forEach(anchors::add);

                    final List<Object> collect = anchors.stream().collect(Collectors.groupingBy(Function.identity(),
                            Collectors.counting()))
                            .entrySet()
                            .stream()
                            .sorted((o1, o2) -> (int) (o2.getValue() - o1.getValue()))
                            .limit(10)
                            .map((Function<Map.Entry<String, Long>, Object>) Map.Entry::getKey)
                            .collect(Collectors.toList());

                    return collect;
                });

        /**
         * part four check anchors
         */

        /*
        stringObjectJavaPairRDD.mapValues((org.apache.spark.api.java.function.Function<Object, Object>) o -> {
            List<String> a = (List<String>) o;
            StringBuilder stringBuilder = new StringBuilder();
            for (String s : a) {
                stringBuilder.append(s + " - ");
            }
            return stringBuilder.toString();
        }).saveAsTextFile("hdfs://hitler:9000/test10");
        */

        /**
         * part five test elastic search
         */

//        final JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
//        parallelize.foreach((VoidFunction<Integer>) integer -> {
//            //Create Request
//            final UpdateRequest request = new UpdateRequest(
//                    "posts",
//                    "doc",
//                    integer.toString());
//
//            //Create document with ContentBuilder
//            XContentBuilder builder = XContentFactory.jsonBuilder();
//            builder.startObject();
//            {
//                builder.field("test", integer.toString());
//            }
//            builder.endObject();
//
//            //bind document to request
//            request.doc(builder);
//
//            client.updateAsync(request, new ActionListener<UpdateResponse>() {
//            @Override
//            public void onResponse(UpdateResponse updateResponse) {
//                System.out.println("good!" + integer.toString());
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                System.out.println("bad!" + integer.toString());
//            }
//        });
//        });

        /**
         * Put to elastic search
         */



        final long count = stringObjectJavaPairRDD.count();



        stringObjectJavaPairRDD.foreach(stringObjectTuple2 -> {
            final List<String> list = stringObjectTuple2._2;
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                stringBuilder.append(list.get(i) + " ");
            }
            final String id = DigestUtils.md5Hex(stringObjectTuple2._1 + 1);

            //Create Request
            final UpdateRequest request = new UpdateRequest(
                    "jimbo5index",
                    "_doc",
                    id);

            //Create document with ContentBuilder
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("Anchors", stringBuilder.toString());
            }
            builder.endObject();

            final XContentBuilder defualt = XContentFactory.jsonBuilder();
            defualt.startObject();
            {
                defualt.field("exist", false);
            }
            defualt.endObject();

            //bind document to request
            request.upsert(defualt);
            request.doc(builder);

            final UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
            successful.add(1);
//            client.updateAsync(request, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
//                @Override
//                public void onResponse(UpdateResponse updateResponse) {
//                    successful.add(1);
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    failure.add(1);
//                }
//            });
        });

        while (true) {
            Thread.sleep(3000);
            System.out.println("successful: " + successful.value() + " failure: " + failure.value() + " ?= " + count);
            if (successful.value() + failure.value() == count) {
                client.close();
                break;
            }
        }

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
