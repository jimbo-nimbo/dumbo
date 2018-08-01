package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.*;

public class LruCacheTest {

  LruCache lruCache;
  static final String  GOOGLE = "google.com";
  static final String  YAHOO = "yahoo.com";
  @Before
  public void setup(){
    lruCache = LruCache.getInstance();
    lruCache.clear();
  }

  @Test
  public void getInstance() {
    assertNotNull(lruCache);
  }

  @Test
  public void add() throws CloneNotSupportedException {
      lruCache.add(GOOGLE);
  }

  @Test(expected = CloneNotSupportedException.class)
  public void testDuplicURL() throws CloneNotSupportedException {
    lruCache.add(GOOGLE);
    lruCache.add(GOOGLE);
  }

  @Test
  public void testUniqAddURL() throws CloneNotSupportedException {
    lruCache.add(GOOGLE);
    lruCache.add(YAHOO);
  }
}