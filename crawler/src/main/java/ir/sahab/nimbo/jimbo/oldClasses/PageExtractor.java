package ir.sahab.nimbo.jimbo.oldClasses;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.hbase.HBase;
import ir.sahab.nimbo.jimbo.parser.Link;
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
import java.util.concurrent.atomic.AtomicLong;

public class PageExtractor implements Runnable {
    private final String topic;
    private final Producer<Long, String> producer;

    private final ArrayBlockingQueue<Document> queue;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    public static AtomicLong pageCounter = new AtomicLong(0l);

    PageExtractor(String topic, Producer<Long, String> producer,
                  ArrayBlockingQueue<Document> queue, ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue) {
        this.topic = topic;
        this.producer = producer;
        this.queue = queue;
        this.elasticQueue = elasticQueue;
    }

    String extractDescriptionMeta(Document doc) {
        Elements metaTags = doc.getElementsByTag("meta");
        for (Element metaTag : metaTags) {
            String name = metaTag.attr("name");
            String content = metaTag.attr("content");
            if (name.equals("description"))
                return content;
        }
        return null;
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
                // TODO: log!
                System.err.println("bad url" + href);
            }
        }
        return links;
    }

    private void sendLinksToKafka(List<Link> links) {
        for (Link link : links) {
            ProducerRecord<Long, String> record =
                    new ProducerRecord<>(topic, null, link.getHref().toString());

            producer.send(record);
        }
    }

    private void sendLinksToElastic(Document document)
    {
        elasticQueue.add(new ElasticsearchWebpageModel(document.location(), document.text(),
                document.title(), extractDescriptionMeta(document)));
    }

    @Override
    public void run() {
        boolean running = true;
        while (running) {
            try {
                final Document doc = queue.take();

                List<Link> links = extractLinks(doc);
                HBase.getInstance().putData(doc.location(), links);

                sendLinksToKafka(links);
                sendLinksToElastic(doc);

                pageCounter.incrementAndGet();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
