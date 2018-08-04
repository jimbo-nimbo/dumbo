package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.kafaconfig.KafkaTopics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

public class PageExtractor implements Runnable {
    private static final String TOPIC = KafkaTopics.URL_FRONTIER.toString();

    private final Producer<Long, String> producer;
    private final ArrayBlockingQueue<Document> queue;

    public PageExtractor(Producer<Long, String> producer, ArrayBlockingQueue<Document> queue) {
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
            producer.send(record);
        }
    }

    @Override
    public void run() {
        while (true) {
            if (queue.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Document doc = queue.poll();
            sendLinksToKafka(extractLinks(doc));
        }

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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Link link = (Link) o;
            return Objects.equals(href, link.href) &&
                    Objects.equals(text, link.text);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(href, text);
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Metadata metadata = (Metadata) o;
            return Objects.equals(name, metadata.name) &&
                    Objects.equals(property, metadata.property) &&
                    Objects.equals(content, metadata.content);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, property, content);
        }
    }
}
