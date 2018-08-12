package ir.sahab.nimbo.jimbo.crawler;

public class CrawlSetting {

    private int shuffledQueueMaxSize;

    private int rawPagesQueueMaxSize;

    /**
     * constructor for testing
     */
    CrawlSetting(int shuffledQueueMaxSize, int rawPagesQueueMaxSize){
        this.shuffledQueueMaxSize = shuffledQueueMaxSize;
        this.rawPagesQueueMaxSize = rawPagesQueueMaxSize;
    }

    int getShuffledQueueMaxSize() {
        return shuffledQueueMaxSize;
    }

    int getRawPagesQueueMaxSize(){
        return rawPagesQueueMaxSize;
    }
}
