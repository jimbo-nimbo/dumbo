package ir.sahab.nimbo.jimbo.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.UnknownHostException;
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
