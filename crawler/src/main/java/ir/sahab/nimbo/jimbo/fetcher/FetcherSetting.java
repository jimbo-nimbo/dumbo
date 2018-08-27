package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.main.Setting;

public class FetcherSetting extends Setting{

    private final int fetcherThreadCount;
    private final int timout;

    public FetcherSetting() {
        super("fetcher");

        fetcherThreadCount = Integer.parseInt(properties.getProperty("fetcher_thread_count"));
        timout = Integer.parseInt(properties.getProperty("timout"));
    }

    public FetcherSetting(int fetcherThreadCount) {
        super("fetcher");
        this.fetcherThreadCount = fetcherThreadCount;
        timout = 30000;
    }

    public int getTimeout() {
        return timout;
    }

    public int getFetcherThreadCount() {
        return fetcherThreadCount;
    }
}
