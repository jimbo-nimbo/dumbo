package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LruCacheTest {

  private LruCache lruCache;
  private static final String  GOOGLE = "google.com";
  private static final String  YAHOO = "yahoo.com";

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
  public void add(){
      assertTrue(lruCache.add(GOOGLE));
  }

  @Test
  public void testDuplicateURL(){
    assertTrue(lruCache.add(GOOGLE));
    assertFalse(lruCache.add(GOOGLE));
  }

  @Test
  public void testUniqueAddURL(){
    assertTrue(lruCache.add(GOOGLE));
    assertTrue(lruCache.add(YAHOO));
  }

  @Test
  public void testExpirationTime(){
      assertTrue(lruCache.add(GOOGLE));
      try {
          TimeUnit.SECONDS.sleep(31);
      } catch (InterruptedException i) {
          Assert.fail();
      }
      assertTrue(lruCache.add(GOOGLE));

  }

    @Test
    public void testYetToExpire(){
        assertTrue(lruCache.add(GOOGLE));
        try {
            TimeUnit.SECONDS.sleep(29);
        } catch (InterruptedException i) {
            Assert.fail();
        }
        assertFalse(lruCache.add(GOOGLE));

    }

  @Test
  public void readProp() {
    assertEquals(30, lruCache.getDuration());
      assertEquals(10000, lruCache.getMaxCacheSize());
  }

}