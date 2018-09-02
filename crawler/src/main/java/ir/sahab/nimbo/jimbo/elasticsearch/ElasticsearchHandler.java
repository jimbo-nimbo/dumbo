package ir.sahab.nimbo.jimbo.elasticsearch;

import ir.sahab.nimbo.jimbo.ElasticClientBuilder;
import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.metrics.Metrics;
import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchHandler{

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHandler.class);
    private final ElasticsearchSetting elasticsearchSetting;
    private static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;
    private final ElasticWorker[] elasticWorkers;

    public ElasticsearchHandler(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                                ElasticsearchSetting elasticsearchSetting){
        this.elasticsearchSetting = elasticsearchSetting;
        ElasticsearchHandler.elasticQueue = elasticQueue;
        elasticWorkers = new ElasticWorker[elasticsearchSetting.getNumberOfThreads()];
    }

    public void runWorkers() throws ConnectException {
        for(int i = 0; i < elasticsearchSetting.getNumberOfThreads(); i++) {
            try {
                elasticWorkers[i] = new ElasticWorker(elasticQueue, elasticsearchSetting);
                elasticWorkers[i].start();
            } catch (UnknownHostException e) {
                logger.error("Could not connect to ElasticSearch");
                throw new ConnectException("Could not connect to ElasticSearch");            }
        }

    }

}
