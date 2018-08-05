package ir.sahab.nimbo.jimbo.fetcher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static ir.sahab.nimbo.jimbo.fetcher.Validate.isEnglish;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidateTest {

    @Test
    public void isEnglishTest() {
        try {
            String temp = Jsoup.connect("https://stackoverflow.com/").get().text();
            Date f = new Date(System.currentTimeMillis());
            assertTrue(isEnglish(temp));
            Date s = new Date(System.currentTimeMillis());
            assertFalse(isEnglish("renمنشسیتبمنaksdjfl jslkfajslk jt نتشسبمنت شسنمت  شتمنسی"));
            System.err.println("time of isenglish time : " + String.valueOf(s.getTime() - f.getTime()));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void banInitialListTest(){
        Validate.isValid(new Document("https://stackoverflow.com/"));
        assertEquals(Validate.banWords.size(), 3);
    }
}