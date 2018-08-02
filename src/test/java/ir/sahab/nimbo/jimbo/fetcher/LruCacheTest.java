package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

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
  public void testDuplicateURL() throws CloneNotSupportedException {
    lruCache.add(GOOGLE);
    lruCache.add(GOOGLE);
  }

  @Test
  public void testUniqueAddURL() throws CloneNotSupportedException {
    lruCache.add(GOOGLE);
    lruCache.add(YAHOO);
  }

  @Test
  public void testExpirationTime() throws CloneNotSupportedException {
      lruCache.add(GOOGLE);
      try {
          TimeUnit.SECONDS.sleep(30);
      } catch (InterruptedException i) {
          Assert.fail();
      }
      lruCache.add(GOOGLE);

  }

    @Test(expected = CloneNotSupportedException.class)
    public void testYetToExpire() throws CloneNotSupportedException {
        lruCache.add(GOOGLE);
        try {
            TimeUnit.SECONDS.sleep(29);
        } catch (InterruptedException i) {
            Assert.fail();
        }
        lruCache.add(GOOGLE);

    }
}