package ir.sahab.nimbo.jimbo.fetcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/** @Author : nimac */
public class LruCache {
  static final int maxCacheSize = 100000;
  private static LruCache lruCache = null;
  private Object lock = new Object();
  Cache<String, Integer> cache =
      Caffeine.newBuilder()
          .maximumSize(maxCacheSize)
          .expireAfterWrite(30, TimeUnit.SECONDS)
          .build();

  private LruCache() {}

  synchronized public static LruCache getInstance() {
    if (lruCache == null) lruCache = new LruCache();
    return lruCache;
  }

  /** if domain is in cache throw exception, else add it */

    /**
     *
     * @param url : domain of site
     * @throws CloneNotSupportedException
     */
  public void add(String url) throws CloneNotSupportedException {
    if (cache.getIfPresent(url) != null) throw new CloneNotSupportedException("url is in LRU");
    synchronized (lock) {
      cache.put(url, 1);
    }
  }
  public void clear(){
      cache.invalidateAll();
  }
}
