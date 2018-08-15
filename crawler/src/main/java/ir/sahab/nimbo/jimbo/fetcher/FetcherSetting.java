package ir.sahab.nimbo.jimbo.fetcher;

public class FetcherSetting {
    private final int fetcherThreadCount;

    public FetcherSetting(int fetcherThreadCount) {
        this.fetcherThreadCount = fetcherThreadCount;
    }

    public int getFetcherThreadCount() {
        return fetcherThreadCount;
    }
}
