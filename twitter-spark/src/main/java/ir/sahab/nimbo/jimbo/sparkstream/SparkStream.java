package ir.sahab.nimbo.jimbo.sparkstream;

import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Serializable;
import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import javax.xml.soap.Text;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static ir.sahab.nimbo.jimbo.main.Config.*;

public class SparkStream implements Serializable {

    private JavaStreamingContexSerializable jssc;

    public SparkStream() {
        initialize();
    }


    private void initialize() {
        System.setProperty("twitter4j.oauth.consumerKey", TWITTER_CONSUMER_KEY);
        System.setProperty("twitter4j.oauth.consumerSecret", TWITTER_CONSUMER_SECRET);
        System.setProperty("twitter4j.oauth.accessToken", TWITTER_ACCESS_TOKEN);
        System.setProperty("twitter4j.oauth.accessTokenSecret", TWITTER_ACCESS_TOKEN_SECRET);
        SparkConf sparkConf = new SparkConf().setAppName("twitterApp");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ALL");
        jssc = new JavaStreamingContexSerializable(sparkContext, new Duration(3000));
        //jssc = new JavaStreamingContext(conf, Durations.seconds(1L));
    }

    void wordCount() {
        //JavaReceiverInputDStream<String> lines = TwitterUtils.createStream(jssc);
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void getTrend(){
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, new String[]{"en"});
        JavaDStream<String> statuses = twitterStream.flatMap((FlatMapFunction<Status, String>) status -> {
            ArrayList<String> list = new ArrayList<>();
            for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
                list.add(hashtagEntity.getText());
            }
            return list.iterator();
        });
        JavaPairDStream<String, Integer> numbering = statuses.mapToPair((PairFunction<String, String, Integer>)
                s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> counting = numbering.reduceByKey((Function2<Integer, Integer, Integer>)
                (integer, integer2) -> integer + integer2);
        //counting.dstream().saveAsTextFiles("hdfs://nimbo1:9000/spark/",);
        counting.saveAsHadoopFiles("hdfs://nimbo1:9000/spark/","txt", Text.class,
                IntWritable.class, TextOutputFormat.class);
        //counting.dstream().saveAsTextFiles("nimac",".rawFile");
//        counting.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//            @Override
//            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
//                for(Map.Entry<String, Integer> entry : stringIntegerJavaPairRDD.collectAsMap().entrySet()){
//                    System.out.println(entry.getKey() + " " + entry.getValue());
//                }
//            }
//        });
        //counting.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
