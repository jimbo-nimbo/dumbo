package ir.sahab.nimbo.jimbo.twitter;

import ir.sahab.nimbo.jimbo.main.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import static ir.sahab.nimbo.jimbo.main.Config.*;

public class TwitterStreamApi {


    TwitterStream twitterStream;
    StatusListener jimboListener;
    private Long counter = 0L;

    public TwitterStreamApi(){
        initialize();



//        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTwitterHelloWorldExample");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));
//        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
        //streamFeed();

    }

    void initialize(){
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
                .setOAuthConsumerSecret(TWITTER_CONSUMER_SECRET)
                .setOAuthAccessToken(TWITTER_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(TWITTER_ACCESS_TOKEN_SECRET);
        twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        twitterStream.addListener(new JimboStatusListner());
        twitterStream.sample("en");
    }

    private class JimboStatusListner implements StatusListener{

        public void onException(Exception e) {
            Logger.getInstance().debugLog("twitter Exception : " + e.getMessage());
        }

        public void onStatus(Status status) {
            counter++;
            System.out.println(status.getText());
        }
        
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        }

        public void onTrackLimitationNotice(int i) {
        }

        public void onScrubGeo(long l, long l1) {
        }

        public void onStallWarning(StallWarning stallWarning) {
        }
    }

    public long getCounter() {
        return counter;
    }
    public void getSpeedPerSecond(){
        Long before = getCounter();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println(getCounter() - before);
    }
}
