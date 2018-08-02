package ir.sahab.nimbo.jimbo.fetcher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.swing.plaf.PanelUI;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class FetcherThread implements Runnable{
    private URL url;
    private Document body;

    /**
     * Created by Pooya
     * @param url
     * @throws MalformedURLException
     */
    public FetcherThread(String url) throws MalformedURLException {
        this.url = new URL(url);
    }

    @Override
    public void run() {
        try {
            LruCache.getInstance().add(url.getHost());
            System.out.println(url.toString());
            body = Jsoup.connect(url.toString()).get();
            // send (body) to kafka
        } catch (CloneNotSupportedException c) {
            c.printStackTrace();
        } catch (IOException i) {
            //TODO log
            i.printStackTrace();
        }
    }


}
