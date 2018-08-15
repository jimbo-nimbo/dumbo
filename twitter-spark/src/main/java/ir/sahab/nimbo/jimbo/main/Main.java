package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.sparkstream.SparkStream;
import ir.sahab.nimbo.jimbo.twitter.TwitterStreamApi;

public class Main {
    public static void main(String[] args){
        SparkStream sparkStream = new SparkStream();
        sparkStream.getTrend();
    }
}
