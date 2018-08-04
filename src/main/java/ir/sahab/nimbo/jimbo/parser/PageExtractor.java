package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaTopics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class PageExtractor implements Runnable {
    private static final Producer<Long, String> PRODUCER = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());
    private static final String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    private final Document doc;

    PageExtractor(Document doc) {
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
                    new ProducerRecord<>(TOPIC, null, link.getHref().toString());
            PRODUCER.send(record);
        }
    }

    @Override
    public void run() {
        sendLinksToKafka(extractLinks());
    }

    class Link {
        private URL href;
        private String text;

        Link(URL href, String text) {
            this.href = href;
            this.text = text;
        }

        URL getHref() {
            return href;
        }

        public String getText() {
            return text;
        }
    }

    class Metadata {
        private String name;
        private String property;
        private String content;

        Metadata(String name, String property, String content) {
            this.name = name;
            this.property = property;
            this.content = content;
        }

        String getName() {
            return name;
        }

        String getProperty() {
            return property;
        }

        String getContent() {
            return content;
        }
    }
}
