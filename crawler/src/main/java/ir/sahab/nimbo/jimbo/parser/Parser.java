package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.Validate;
import ir.sahab.nimbo.jimbo.hbase.HBase;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import ir.sahab.nimbo.jimbo.main.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Parser {
    private final ArrayBlockingQueue<String> webPages;

    private final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    Parser(ArrayBlockingQueue<String> webPages) {

        this.webPages = webPages;
    }

}

class Worker implements Runnable {

    private final Producer<String, String> producer;
    private final ArrayBlockingQueue<String> webPage;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private boolean running = true;

    Worker(Producer<String, String> producer,
           ArrayBlockingQueue<String> webPage, ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue) {
        this.producer = producer;
        this.webPage = webPage;
        this.elasticQueue = elasticQueue;
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        while (running) {
            try {
                final Document document = Jsoup.parse(webPage.take());
                if (Validate.isValidBody(document)) {
                    synchronized (producer) {
                        producer.send(
                                new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null));
                    }
                }

                List<Link> links = extractLinks(document);
                HBase.getInstance().putData(document.location(), links);

                sendLinksToKafka(links);
                submitToElastic(document);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String getDescriptionTag(Document document){
        Elements metaTags = document.getElementsByTag("meta");

        for (Element metaTag : metaTags) {
            String name = metaTag.attr("name");
            String content = metaTag.attr("content");
            if (name.equalsIgnoreCase("description"))
                return content;
        }
        return "";
    }

    private void submitToElastic(Document document) throws InterruptedException {
        elasticQueue.put(new ElasticsearchWebpageModel(
                "", document.text(), document.title(), getDescriptionTag(document)));
    }

    private List<Link> extractLinks(Document document) {
        final Elements aTags = document.getElementsByTag("a");
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
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null, link.getHref().toString());

            producer.send(record);
        }
    }
}
