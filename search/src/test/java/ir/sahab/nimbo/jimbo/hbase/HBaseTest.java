package ir.sahab.nimbo.jimbo.hbase;

import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseTest {

    @Test
    public void getNumberOfReferences() {
        System.err.println(HBase.getInstance().getNumberOfReferences("https://celia14.itch.io/hello-hello"));
    }
}