package ir.sahab.nimbo.jimbo.fetcher;

import java.net.MalformedURLException;
import java.net.URL;

public class FetcherThread {
    private URL url;

    /**
     * Created by Pooya
     * @param url
     * @throws MalformedURLException
     */
    public FetcherThread(String url) throws MalformedURLException {
        this.url = new URL(url);

    }
}
