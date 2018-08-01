package ir.sahab.nimbo.jimbo.fetcher;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.TimeUnit;

/**
 * @Author : nimac
 */
public class LruCache {
    final static int maxCacheSize = 100000;
    Cache<Integer, String> cache = Caffeine.newBuilder().maximumSize(maxCacheSize).expireAfterWrite(30, TimeUnit.SECONDS).build();
    private static LruCache lruCache = null;

    public static LruCache getInstance(){
        if(lruCache == null)
            lruCache = new LruCache();
        return lruCache;
    }

    private LruCache(){

    }

    /**
     * if domain is in cache throw exception, else add it
     */
    public void add(String url) throws CloneNotSupportedException{

    }

}
