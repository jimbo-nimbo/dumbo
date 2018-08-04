package ir.sahab.nimbo.jimbo.fetcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** @Author : nimac
 * singleton pattern
 * cache the domains
 */
public class LruCache {
  private int maxCacheSize;
  private int duration;
  private static final String  PROP_NAME = "lru.properties";
  private static LruCache lruCache = null;
  private Object lock = new Object();
  private Cache<String, Integer> cache = null;


  private LruCache() {
    Properties properties = new Properties();
    try {
      properties.load(getClass().getClassLoader().getResourceAsStream(PROP_NAME));
      maxCacheSize = Integer.valueOf(properties.getProperty("max_cache"));
      duration = Integer.valueOf(properties.getProperty("duration"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    cache = Caffeine.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterWrite(duration, TimeUnit.SECONDS)
            .build();
  }

  synchronized public static LruCache getInstance() {
    if (lruCache == null) lruCache = new LruCache();
    return lruCache;
  }

  /** if domain is in cache throw exception, else add it */

    /**
     * add a site domain to cache, if it exist, it throws exception, else it add it
     * cache has a capacity and timeout for each domain
     * if it goes more than capacity, it remove element in a strange way to optimum itself
     * @param url : domain of site
     * @throws CloneNotSupportedException
     */
  public void add(String url) throws CloneNotSupportedException {
    if (cache.getIfPresent(url) != null) throw new CloneNotSupportedException("url is in LRU");
    synchronized (lock) {
      cache.put(url, 1);
    }
  }

  public int getMaxCacheSize() {
    return maxCacheSize;
  }

  public int getDuration() {
    return duration;
  }

  /**
   * clear all sites in cache
   */
  public void clear(){
      cache.invalidateAll();
  }
}
