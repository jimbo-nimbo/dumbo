package ir.sahab.nimbo.jimbo.fetcher;

import org.jsoup.Jsoup;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.*;

public class ValidateTest {

  private static Validate validate;
  @BeforeClass
  public static void prep(){
    validate = new Validate();
  }
  @Test
  public void isEnglish() {
    try {
      String temp = Jsoup.connect("https://stackoverflow.com/").get().text(); 
      Date f = new Date(System.currentTimeMillis());
      assertTrue(validate.isEnglish(temp));
      Date s = new Date(System.currentTimeMillis());
      assertFalse(validate.isEnglish("renمنشسیتبمنaksdjfl jslkfajslk jt نتشسبمنت شسنمت  شتمنسی"));
      System.err.println("time of isenglish time : " + String.valueOf(s.getTime() - f.getTime()));
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}