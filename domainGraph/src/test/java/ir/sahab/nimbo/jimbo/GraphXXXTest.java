package ir.sahab.nimbo.jimbo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class GraphXXXTest {

    @Test
    public void testJsc() {
        final SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("GraphXTest");
        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        final List<Integer> integers = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        final JavaRDD<Integer> parallelize = jsc.parallelize(integers);

        final List<Integer> collect = parallelize.collect();
        for (int i = 0; i < collect.size(); i++) {
            assertEquals(collect.get(i).intValue(), i);
        }

    }
}