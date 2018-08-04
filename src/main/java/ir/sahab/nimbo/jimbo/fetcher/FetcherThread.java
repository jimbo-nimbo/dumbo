package ir.sahab.nimbo.jimbo.fetcher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.swing.plaf.PanelUI;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class FetcherThread implements Runnable {
    private URL url;

    /**
     * Created by Pooya
     * @param url
     * @throws MalformedURLException
     */
    public  FetcherThread(String url) throws MalformedURLException {
        this.url = new URL(url);
    }

    /**
     * author: Pya
     *
     * @return The HTML as a document
     * @throws CloneNotSupportedException when domain already exists in LRU
     * @throws IOException when URL does not link to a valid page
     */
    Document getUrlBody() throws CloneNotSupportedException, IOException {
        LruCache.getInstance().add(url.getHost());
        return Jsoup.connect(url.toString()).get();
    }

    @Override
    public void run() {
        try {
            Document body = getUrlBody();
            // send (body) to parser
        } catch (CloneNotSupportedException c) {
            //TODO send back to Kafka
            c.printStackTrace();
        } catch (IOException i) {
            //TODO log
            i.printStackTrace();
        }
    }


}
