package ir.sahab.nimbo.jimbo.fetcher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static ir.sahab.nimbo.jimbo.fetcher.Validator.isEnglish;
import static org.junit.Assert.*;

public class ValidatorTest {

    private static Document banSite;
    private static Document goodSite;
    private static String banSiteText;
    private static String goodSiteText;

    @Before
    public void prep() throws IOException {
        banSite = Jsoup.connect("http://www.porn.com").get();
        goodSite = Jsoup.connect("http://hbase.apache.org").get();
        banSiteText = banSite.text();
        goodSiteText = goodSite.text();
        Validator.init();
    }

    @Test
    public void isEnglishTest() {

        assertTrue(isEnglish(goodSiteText));
        Date f = new Date(System.currentTimeMillis());
        isEnglish(goodSiteText);
        Date s = new Date(System.currentTimeMillis());
        System.err.println("time of isEnglish time : " + String.valueOf(s.getTime() - f.getTime()));
        assertFalse(isEnglish("renمنشسیتبمنaksdjfl jslkfajslk jt نتشسبمنت شسنمت  شتمنسی"));
    }

    @Test
    public void banInitialListTest() {
        Validator.isValidBody(goodSite);
        assertEquals(Validator.banWords.size(), 5);
    }

    @Test
    public void benchMarkTest(){
        assertTrue(Validator.isNotBanBody(goodSite.text()));
        assertTrue(Validator.isNotBan(goodSite));
        assertTrue(Validator.isValidBody(goodSite));
        Date f = new Date(System.currentTimeMillis());
        Validator.isNotBanBody(goodSite.text());
        Date s = new Date(System.currentTimeMillis());
        System.err.println("time of notBanBody time : " + String.valueOf(s.getTime() - f.getTime()));
        Validator.isNotBan(goodSite);
        s = new Date(System.currentTimeMillis());
        System.err.println("time of NotBan time : " + String.valueOf(s.getTime() - f.getTime()));
        Validator.isValidBody(goodSite);
        s = new Date(System.currentTimeMillis());
        System.err.println("time of ValidBody time : " + String.valueOf(s.getTime() - f.getTime()));
        f = new Date(System.currentTimeMillis());
        Validator.allValidation(goodSite);
        s = new Date(System.currentTimeMillis());
        System.err.println("time of AllValid time : " + String.valueOf(s.getTime() - f.getTime()));


    }

    @Test
    public void isNotBanBodyTest() {
        Date f = new Date(System.currentTimeMillis());
        assertTrue(Validator.isNotBanBody(goodSite.text()));
        Date s = new Date(System.currentTimeMillis());
        System.err.println("time of valid time : " + String.valueOf(s.getTime() - f.getTime()));
    }

    @Test
    public void isValidTest() {
        Date f = new Date(System.currentTimeMillis());
        assertFalse(Validator.isValidBody(banSite));
        Date s = new Date(System.currentTimeMillis());
        System.err.println("time of valid time : " + String.valueOf(s.getTime() - f.getTime()));
    }

    @Test
    public void notBanTest() {
        assertTrue(Validator.isNotBan(goodSite));
    }

    @Test
    public void banTest() {
        Date f = new Date(System.currentTimeMillis());
        assertFalse(Validator.isNotBan(banSite));
        Date s = new Date(System.currentTimeMillis());
        System.err.println("time of banTest time : " + String.valueOf(s.getTime() - f.getTime()));
    }
}