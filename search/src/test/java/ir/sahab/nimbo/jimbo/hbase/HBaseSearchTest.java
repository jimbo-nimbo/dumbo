package ir.sahab.nimbo.jimbo.hbase;

import org.junit.Test;

import java.util.Arrays;

public class HBaseSearchTest {

    @Test
    public void getNumberOfReferences() {
        System.err.println(HBaseSearch.getInstance().getNumberOfReferences(Arrays.asList("https://scenesofeating.com/category/language/"))[0]);
    }
}