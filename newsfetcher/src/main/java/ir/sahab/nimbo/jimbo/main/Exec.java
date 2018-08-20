package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticCannotLoadException;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.rss.RssFeed;
import ir.sahab.nimbo.jimbo.rss.RssFeedMessage;
import ir.sahab.nimbo.jimbo.rss.RssFeedParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Exec {
    static ArrayBlockingQueue<ElasticsearchWebpageModel> blockingQueue = new ArrayBlockingQueue<>(1000);
    static String fetch(String url, NewsSite site) {
        try {
            Document doc = Jsoup.connect(url).validateTLSCertificates(false).timeout(3000).get();
            Elements divs = doc.select(site.getTag() + "[" + site.getAttribute() + "]");
            for (Element div : divs) {
                if (div.attr(site.getAttribute()).contains(site.getAttributeValue())){
                    return div.text();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static void main(String[] args) throws ElasticCannotLoadException, IOException {
//        final ElasticsearchThreadFactory elasticsearchThreadFactory = new ElasticsearchThreadFactory(blockingQueue);
//        elasticsearchThreadFactory.createNewThread();
        final List<NewsSite> urls = Seeder.getInstance().getUrls();
        int rssNumber = urls.size();
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
        HashSet<String> rssMessages = new HashSet<>();
        ex.scheduleAtFixedRate(new Runnable() {
            private int i = 0;
            @Override
            public void run() {
                new Thread(() -> {
                    NewsSite url = null;
                    try {
                        url = urls.get(i);
                    } catch (IndexOutOfBoundsException e) {
                        i = 0;
                        url = urls.get(i);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    RssFeedParser parser = new RssFeedParser(url.getUrl());
                    RssFeed rssFeed = parser.readFeed();
                    for(RssFeedMessage message: rssFeed.getMessages()){
                        if(rssMessages.add(message.getLink())) {
                            String text = fetch(message.getLink(), url);
                            if(text != null) {
                                blockingQueue.add(new ElasticsearchWebpageModel(message.getLink(), text, message.getTitle(), message.getDescription()));
                            }
                        }
                    }
                    i++;}
                ).run();
            }
        }, 0, 60/rssNumber, TimeUnit.SECONDS);
        while(true)
        {
            try{
            System.out.println(blockingQueue.take().getArticle());}
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
