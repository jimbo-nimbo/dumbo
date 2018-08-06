package ir.sahab.nimbo.jimbo.parser;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PageExtractor implements Runnable {
    private final String topic;
    private final Producer<Long, String> producer;
    private final ArrayBlockingQueue<Document> queue;

    public static Long pageCounter = 0l;

    PageExtractor(String topic, Producer<Long, String> producer, ArrayBlockingQueue<Document> queue) {
        this.topic = topic;
        this.producer = producer;
        this.queue = queue;
    }

    List<Metadata> extractMetadata(Document doc) {
        Elements metaTags = doc.getElementsByTag("meta");
        List<Metadata> metadatas = new ArrayList<>();
        for (Element metaTag : metaTags) {
            String name = metaTag.attr("name");
            String property = metaTag.attr("property");
            String content = metaTag.attr("content");
            metadatas.add(new Metadata(name, property, content));
        }
        return metadatas;
    }

    List<Link> extractLinks(Document doc) {
        final Elements aTags = doc.getElementsByTag("a");
        final List<Link> links = new ArrayList<>();

        for (Element aTag : aTags) {
            String href = aTag.absUrl("href");
            String text = aTag.text();
            if (href == null || href.equals(""))
                continue;
            try {
                links.add(new Link(new URL(href), text));
            } catch (MalformedURLException e) {
                System.err.println("bad url" + href);
            }
        }
        return links;
    }

    private void sendLinksToKafka(List<Link> links) {
        pageCounter++;
        for (Link link : links) {
            ProducerRecord<Long, String> record =
                    new ProducerRecord<>(topic, null, link.getHref().toString());

            producer.send(record);
        }
    }

    private void sendLinksToHbase(List<Link> links)
    {

    }

    @Override
    public void run() {
        boolean running = true;
        while (running) {
            try {
                final Document doc = queue.take();
                List<Link> links = extractLinks(doc);
                sendLinksToKafka(links);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
