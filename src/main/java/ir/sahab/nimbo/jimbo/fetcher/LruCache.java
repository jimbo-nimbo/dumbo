package ir.sahab.nimbo.jimbo.fetcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * singleton pattern
 * cache the domains
 * //TODO: handle exception
 */
class LruCache
{
    private int maxCacheSize;
    private int duration;
    private static final String PROP_NAME = "lru.properties";
    private static LruCache lruCache = null;
    private Cache<String, Integer> cache;


    private LruCache()
    {
        Properties properties = new Properties();
        try
        {
            properties.load(getClass().getClassLoader().getResourceAsStream(PROP_NAME));
            maxCacheSize = Integer.parseInt(properties.getProperty("max_cache"));
            duration = Integer.parseInt(properties.getProperty("duration"));
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterWrite(duration, TimeUnit.SECONDS)
                .build();
    }

    synchronized static LruCache getInstance()
    {
        if (lruCache == null){
            lruCache = new LruCache();
        }
        return lruCache;
    }


    /**
     * add a site domain to cache, if it exist, it throws exception, else it add it
     * cache has a capacity and timeout for each domain
     * if it goes more than capacity, it remove element in a strange way to optimum itself
     *
     * @param url
     *      domain of site
     * @throws CloneNotSupportedException
     *      if domain is in cache throw exception, else add it
     */
    synchronized void add(String url) throws CloneNotSupportedException
    {

        if (cache.getIfPresent(url) != null){
            throw new CloneNotSupportedException("url is in LRU");
        }
            cache.put(url, 1);
    }

    int getMaxCacheSize()
    {
        return maxCacheSize;
    }

    int getDuration()
    {
        return duration;
    }

    /**
     * clear all sites in cache
     */
    void clear()
    {
        cache.invalidateAll();
    }
}
