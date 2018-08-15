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
import java.util.concurrent.atomic.AtomicInteger;

public class Parser {
    private final ArrayBlockingQueue<WebPageModel> webPages;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());
    private final ParserSetting parserSetting;

    private final Worker[] workers;

    public static AtomicInteger parsedPages = new AtomicInteger(0);

    public Parser(ArrayBlockingQueue<WebPageModel> webPages,
                  ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue, ParserSetting parserSetting) {

        this.webPages = webPages;
        this.elasticQueue = elasticQueue;
        this.parserSetting = parserSetting;

        this.workers = new Worker[parserSetting.getParserThreadCount()];
        for (int i = 0; i < workers.length; i++) {
                workers[i] = new Worker(producer, webPages, elasticQueue);
        }
    }

    public void runWorkers() {
        for (Worker worker : workers) {
            new Thread(worker).start();
        }
    }

}

class Worker implements Runnable {

    private final Producer<String, String> producer;
    private final ArrayBlockingQueue<WebPageModel> webPage;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;

    private boolean running = true;

    Worker(Producer<String, String> producer,
           ArrayBlockingQueue<WebPageModel> webPage, ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue) {
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
//            System.out.println("parser started");
            try {
                WebPageModel model = webPage.take();
//                System.out.println(model.getLink() + "=============================");
                final Document document = Jsoup.parse(model.getHtml());
                if (Validate.allValidation(document)) {
                    Parser.parsedPages.incrementAndGet();
//                    sendToElastic(model, document);

                    List<Link> links = extractLinks(document);
                    sendLinksToKafka(links);
//                    HBase.getInstance().putData(model.getLink(), links);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendToElastic(WebPageModel model, Document document) throws InterruptedException {
        String description = "";
        for (Element metaTag: document.getElementsByTag("meta")) {
            if (metaTag.attr("name").equalsIgnoreCase("description")) {
                description = metaTag.attr("content");
                break;
            }
        }
        elasticQueue.put(new ElasticsearchWebpageModel(model.getLink(),
                document.text(), document.getElementsByTag("title").text(), description));
    }

    private void sendLinksToKafka(List<Link> links) {
        for (Link link : links) {
            producer.send(
                    new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null, link.getHref().toString()));
        }
    }

    private List<Link> extractLinks(Document document) {
        final Elements aTags = document.getElementsByTag("a");
        final List<Link> links = new ArrayList<>();

        for (Element aTag : aTags) {
            String href = aTag.absUrl("href");
            if (href == null || href.equals(""))
                continue;
            try {
                links.add(new Link(new URL(href), aTag.text()));
            } catch (MalformedURLException e) {
                //Todo: log
                System.err.println("bad url" + href);
            }
        }
        return links;
    }
}
