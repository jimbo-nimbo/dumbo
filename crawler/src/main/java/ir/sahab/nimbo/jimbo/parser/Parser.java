package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.elasticsearch.ElasticsearchWebpageModel;
import ir.sahab.nimbo.jimbo.hbase.HBaseDataModel;
import ir.sahab.nimbo.jimbo.kafka.KafkaPropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Parser {
    private final ArrayBlockingQueue<WebPageModel> webPages;
    private final ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;
    private final ArrayBlockingQueue<HBaseDataModel> hbaseQueue;

    private final Producer<String, String> producer = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());

    private final ParseWorker[] parseWorkers;

    public static AtomicInteger parsedPages = new AtomicInteger(0);

    public Parser(ArrayBlockingQueue<WebPageModel> webPages,
                  ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                  ArrayBlockingQueue<HBaseDataModel> hbaseQueue,
                  ParserSetting parserSetting) {

        this.webPages = webPages;
        this.elasticQueue = elasticQueue;
        this.hbaseQueue = hbaseQueue;

        this.parseWorkers = new ParseWorker[parserSetting.getParserThreadCount()];
        for (int i = 0; i < parseWorkers.length; i++) {
                parseWorkers[i] = new ParseWorker(producer, webPages, elasticQueue, hbaseQueue);
        }
    }

    public void runWorkers() {
        for (ParseWorker parseWorker : parseWorkers) {
            new Thread(parseWorker).start();
        }
    }

}


