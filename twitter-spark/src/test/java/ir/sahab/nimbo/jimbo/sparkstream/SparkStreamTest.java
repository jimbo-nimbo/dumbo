package ir.sahab.nimbo.jimbo.sparkstream;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.junit.Assert.*;

public class SparkStreamTest {

    SparkStream sparkStream;
    @Before
    public void setUp() throws Exception {
        sparkStream = new SparkStream();
    }

    @Test
    public void initialize() {
    }

    @Test
    public void wordCount() {
    }

    @Test
    public void getTrend() throws InterruptedException{
        sparkStream.getTrend();
        //Thread.sleep(10000);
    }
}