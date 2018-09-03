package ir.sahab.nimbo.jimbo;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD;

public class GraphXXX {

    private final JavaSparkContext jsc;

    GraphXXX() {
        final SparkConf sparkConf = new SparkConf().setAppName(Config.SPARK_APP_NAME);
        this.jsc = new JavaSparkContext(sparkConf);
    }


    public JavaSparkContext getJsc() {
        return jsc;
    }
}
