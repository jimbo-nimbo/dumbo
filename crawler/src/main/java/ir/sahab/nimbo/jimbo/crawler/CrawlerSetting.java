package ir.sahab.nimbo.jimbo.crawler;

import ir.sahab.nimbo.jimbo.main.Setting;

public class CrawlerSetting extends Setting{

    private final int shuffledQueueMaxSize;
    private final int rawPagesQueueMaxSize;
    private final int elasticQueueMaxSize;

    public CrawlerSetting() {
        super("crawler");

        shuffledQueueMaxSize = Integer.parseInt(properties.getProperty("shuffled_queue_max_size"));
        rawPagesQueueMaxSize = Integer.parseInt(properties.getProperty("raw_page_queue_max_size"));
        elasticQueueMaxSize = Integer.parseInt(properties.getProperty("elastic_queue_max_size"));
    }

    /**
     * constructor for testing
     */
    public CrawlerSetting(int shuffledQueueMaxSize, int rawPagesQueueMaxSize, int elasticQueueMaxSize){
        super("crawler");
        this.shuffledQueueMaxSize = shuffledQueueMaxSize;
        this.rawPagesQueueMaxSize = rawPagesQueueMaxSize;
        this.elasticQueueMaxSize = elasticQueueMaxSize;
    }

    public int getElasticQueueMaxSize() {
        return elasticQueueMaxSize;
    }

    int getShuffledQueueMaxSize() {
        return shuffledQueueMaxSize;
    }

    int getRawPagesQueueMaxSize(){
        return rawPagesQueueMaxSize;
    }
}
