package ir.sahab.nimbo.jimbo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class AnchorFinderTest
{
    private static JavaSparkContext jsc;

    @BeforeClass
    public static void createSparkContext() {
        final SparkConf conf = new SparkConf().setAppName(Config.SPARK_APP_NAME).setMaster("spark://hitler:7077");
        jsc = new JavaSparkContext(conf);
    }

    @Test
    public void testSpark() {

        final JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        final List<Integer> collect = parallelize.collect();

        for (int i = 0; i < collect.size(); i++) {
            assertEquals(i, collect.get(i).intValue());
        }

    }

}