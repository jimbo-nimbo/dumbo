package ir.sahab.nimbo.jimbo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.regex.Matcher;

import static org.junit.Assert.*;

public class WordCountTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Test
    public void getCounts() throws IOException {
        final SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(Config.SPARK_APP_NAME);
        final SparkContext sparkContext =  new SparkContext(sparkConf);
        final File file = this.temporaryFolder.newFile();
        Files.write(file.toPath(), "foo bar bar \n bar bar foo".getBytes());

        Map<String, Integer> counts = WordCount.counts(sparkContext, file.getAbsolutePath());

        MatcherAssert.assertThat(counts.get("foo"), CoreMatchers.is(4));
    }
}