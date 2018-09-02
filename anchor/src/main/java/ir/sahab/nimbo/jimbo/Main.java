package ir.sahab.nimbo.jimbo;

import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
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

public class Main {

    public static void wordCount() {
        SparkConf conf = new SparkConf().setAppName("Work Count App");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("hdfs://hitler:9000/testSarb/luremIpsum.txt");
//        hdfs://hitler:9000/testSarb/luremIpsum.txt

        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts =
                words.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        counts.saveAsTextFile("hdfs://hitler:9000/testSarb");
//        hdfs://hitler:9000/output
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

    public static void main(String[] args) throws IOException, InterruptedException {
        NumRefers.extractNumRefers();


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
}
