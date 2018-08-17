package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.Validator;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.main.Config;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

class ParseWorker implements Runnable {

    private final Producer<String, String> producer;
    private final ArrayBlockingQueue<WebPageModel> webPage;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;
    private final ArrayBlockingQueue<HBaseDataModel> hbaseQueue;

    private boolean running = true;

    ParseWorker(Producer<String, String> producer,
                ArrayBlockingQueue<WebPageModel> webPage,
                ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                ArrayBlockingQueue<HBaseDataModel> hbaseQueue) {
        this.producer = producer;
        this.webPage = webPage;
        this.elasticQueue = elasticQueue;
        this.hbaseQueue = hbaseQueue;
    }

    @Override
    public void run() {
        while (running) {
            try {
                WebPageModel model = webPage.take();
                final Document document = Jsoup.parse(model.getHtml());
                if (Validator.allValidation(document)) {
                    Parser.parsedPages.incrementAndGet();
                    sendToElastic(model, document);
                    List<Link> links = extractLinks(document);
                    sendLinksToKafka(links);
                    hbaseQueue.put(new HBaseDataModel(model.getLink(), links));
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
                document.text(), document.title(), description));
    }

    private void sendLinksToKafka(List<Link> links) {
        for (Link link : links) {
            producer.send(
                    new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, null, link.getHref()));
        }
    }

    private List<Link> extractLinks(Document document) {
        final Elements aTags = document.getElementsByTag("a");
        final List<Link> links = new ArrayList<>();

        for (Element aTag : aTags) {
            String href = aTag.absUrl("href");
            if (href == null || href.equals(""))
                continue;
            links.add(new Link(href, aTag.text()));
        }
        return links;
    }

}
