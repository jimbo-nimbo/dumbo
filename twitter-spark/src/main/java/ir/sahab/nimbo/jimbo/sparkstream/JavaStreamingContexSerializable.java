package ir.sahab.nimbo.jimbo.sparkstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Map;

public class JavaStreamingContexSerializable extends JavaStreamingContext implements Serializable {
    public JavaStreamingContexSerializable(StreamingContext ssc) {
        super(ssc);
    }

    public JavaStreamingContexSerializable(String master, String appName, Duration batchDuration) {
        super(master, appName, batchDuration);
    }

    public JavaStreamingContexSerializable(String master, String appName, Duration batchDuration, String sparkHome, String jarFile) {
        super(master, appName, batchDuration, sparkHome, jarFile);
    }

    public JavaStreamingContexSerializable(String master, String appName, Duration batchDuration, String sparkHome, String[] jars) {
        super(master, appName, batchDuration, sparkHome, jars);
    }

    public JavaStreamingContexSerializable(String master, String appName, Duration batchDuration, String sparkHome, String[] jars, Map<String, String> environment) {
        super(master, appName, batchDuration, sparkHome, jars, environment);
    }

    public JavaStreamingContexSerializable(JavaSparkContext sparkContext, Duration batchDuration) {
        super(sparkContext, batchDuration);
    }

    public JavaStreamingContexSerializable(SparkConf conf, Duration batchDuration) {
        super(conf, batchDuration);
    }

    public JavaStreamingContexSerializable(String path) {
        super(path);
    }

    public JavaStreamingContexSerializable(String path, Configuration hadoopConf) {
        super(path, hadoopConf);
    }
}
