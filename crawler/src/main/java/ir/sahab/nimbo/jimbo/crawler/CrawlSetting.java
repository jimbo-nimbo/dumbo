package ir.sahab.nimbo.jimbo.crawler;

public class CrawlSetting {

    private final int shuffledQueueMaxSize;

    private final int rawPagesQueueMaxSize;

    private final int elasticQueueMaxSize;
    /**
     * constructor for testing
     */
    public CrawlSetting(int shuffledQueueMaxSize, int rawPagesQueueMaxSize, int elasticQueueMaxSize){
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
