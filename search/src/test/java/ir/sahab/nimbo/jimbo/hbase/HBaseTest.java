package ir.sahab.nimbo.jimbo.hbase;

import org.junit.Test;

import java.util.Arrays;

public class HBaseTest {

    @Test
    public void getNumberOfReferences() {
        System.err.println(HBase.getInstance().getNumberOfReferences(Arrays.asList("https://celia14.itch.io/hello-hello")));
    }
}