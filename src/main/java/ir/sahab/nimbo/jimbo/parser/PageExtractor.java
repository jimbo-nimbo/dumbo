package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class PageExtractor implements Runnable {
    private static final Producer<Long, String> PRODUCER;
    private static final String KAFKA_TOPIC = "TestTopic";

    static {
        String s = "";
        try {
            s = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        PRODUCER = new KafkaProducer<>(
                KafkaPropertyFactory.getProducerProperties(
                ));
    }

    private final Document doc;

    public PageExtractor(Document doc) {
        this.doc = doc;
    }

    List<Metadata> extractMetadata() {
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

    List<Link> extractLinks() {
        Elements aTags = doc.getElementsByTag("a");
        List<Link> links = new ArrayList<>();

        for (Element aTag : aTags) {
            String href = aTag.absUrl("href");
            String text = aTag.text();
            if (href == null || href.equals(""))
                continue;
            try {
                links.add(new Link(new URL(href), text));
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        return links;
    }

    void sendLinksToKafka(List<Link> links) {
        for (Link link : links) {
            ProducerRecord<Long, String> record =
                    new ProducerRecord<>(KAFKA_TOPIC, null, link.getHref().toString());
            PRODUCER.send(record);
        }
    }

    @Override
    public void run() {
        sendLinksToKafka(extractLinks());
    }
}
