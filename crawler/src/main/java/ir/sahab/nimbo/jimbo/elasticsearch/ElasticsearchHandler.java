package ir.sahab.nimbo.jimbo.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchHandler{

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHandler.class);
    private final ElasticsearchSetting elasticsearchSetting;
    private static ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue;
    private final Worker[] elasticWorkers;

    public ElasticsearchHandler(ArrayBlockingQueue<ElasticsearchWebpageModel> elasticQueue,
                                ElasticsearchSetting elasticsearchSetting){
        this.elasticsearchSetting = elasticsearchSetting;
        ElasticsearchHandler.elasticQueue = elasticQueue;
        elasticWorkers = new Worker[elasticsearchSetting.getNumberOfThreads()];
    }

    public void runWorkers() throws ConnectException {
        for(int i = 0; i < elasticsearchSetting.getNumberOfThreads(); i++) {
                elasticWorkers[i] = new Worker(elasticQueue, elasticsearchSetting);
                new Thread(elasticWorkers[i]).start();
        }

    }

}
