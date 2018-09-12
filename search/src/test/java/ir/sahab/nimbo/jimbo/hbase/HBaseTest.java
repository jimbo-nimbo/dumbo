package ir.sahab.nimbo.jimbo.hbase;

import org.junit.Test;

import java.util.Arrays;

public class HBaseTest {

    @Test
    public void getNumberOfReferences() {
        System.err.println(HBase.getInstance().getNumberOfReferences(Arrays.asList("https://scenesofeating.com/category/language/"))[0]);
    }
}