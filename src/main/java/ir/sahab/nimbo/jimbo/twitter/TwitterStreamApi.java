package ir.sahab.nimbo.jimbo.twitter;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import static ir.sahab.nimbo.jimbo.main.Config.*;
public class TwitterStreamApi {

    public TwitterStreamApi(){

        //TwitterUtils.createStream(jssc);
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));
        System.setProperty("twitter4j.oauth.consumerKey", TWITTER_CONSUMER_KEY);

        System.setProperty("twitter4j.oauth.consumerSecret", TWITTER_CONSUMER_SECRET);

        System.setProperty("twitter4j.oauth.accessToken", TWITTER_ACCESS_TOKEN);

        System.setProperty("twitter4j.oauth.accessTokenSecret", TWITTER_ACCESS_TOKEN_SECRET);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
    }
}
