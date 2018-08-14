package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.main.Setting;

public class FetcherSetting extends Setting{

    private final int fetcherThreadCount;

    public FetcherSetting() {
        super("fetcher");

        fetcherThreadCount = Integer.parseInt(properties.getProperty("fetcher_thread_count"));
    }

    public FetcherSetting(int fetcherThreadCount) {
        super("fetcher");
        this.fetcherThreadCount = fetcherThreadCount;
    }

    public int getFetcherThreadCount() {
        return fetcherThreadCount;
    }
}
