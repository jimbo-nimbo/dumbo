package ir.sahab.nimbo.jimbo.parser;

import com.codahale.metrics.Timer;
import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.fetcher.Validator;
import ir.sahab.nimbo.jimbo.hbase.HBase;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

class ParseWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ParseWorker.class);

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
                Timer.Context parserJobRequestsTimeContex = Metrics.getInstance().parserJobRequestsTime();
                Timer.Context parserTakeWebPageRequestsTimeContext =
                        Metrics.getInstance().parserTakeWebPageRequestsTime();
                WebPageModel model = webPage.take();
                parserTakeWebPageRequestsTimeContext.stop();
                Timer.Context parserJsoupParseRequestsTimeContext =
                        Metrics.getInstance().parserJsoupParseRequestsTime();
                final Document document = Jsoup.parse(model.getHtml());
                parserJsoupParseRequestsTimeContext.stop();
                Metrics.getInstance().markParsedPages();
                if (Validator.allValidation(document)) {
                    Timer.Context parserElasticPutDataRequestsTimeContext =
                            Metrics.getInstance().parserElasticPutDataRequestsTime();
                    sendToElastic(model, document);
                    parserElasticPutDataRequestsTimeContext.stop();
                    Timer.Context parserExtractLinksRequestsTimeContext =
                            Metrics.getInstance().parserExtractLinksRequestsTime();
                    List<Link> links = extractLinks(document);
                    parserExtractLinksRequestsTimeContext.stop();
                    sendLinksToKafka(links);
                    Timer.Context hbasePutDataTimeContext = Metrics.getInstance().hbasePutDataRequestsTime();
                    hbaseQueue.put(new HBaseDataModel(model.getLink(), links));
                    hbasePutDataTimeContext.stop();
                    Metrics.getInstance().markExtractedLinks(links.size());
                } else {
                    Metrics.getInstance().markInvalidDocuments();
                }
                parserJobRequestsTimeContex.stop();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
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
            String url = link.getHref();
            Timer.Context kafkaProduceTimeContext = Metrics.getInstance().kafkaProduceRequestsTime();
            producer.send(new ProducerRecord<>(Config.URL_FRONTIER_TOPIC, DigestUtils.md5Hex(url), url));
            kafkaProduceTimeContext.stop();
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
