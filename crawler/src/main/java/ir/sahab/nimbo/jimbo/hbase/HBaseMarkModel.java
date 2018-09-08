package ir.sahab.nimbo.jimbo.hbase;

public class HBaseMarkModel {
    private final String url;
    private final Long lastSeen;
    private Long duration;
    private String bodyHash;

    public HBaseMarkModel(String url, Long lastSeen, Long duration, String bodyHash) {
        this.url = url;
        this.lastSeen = lastSeen;
        this.duration = duration;
        this.bodyHash = bodyHash;
    }

    public String getUrl() {
        return url;
    }

    public Long getLastSeen() {
        return lastSeen;
    }

    public Long getDuration() {
        return duration;
    }

    public String getBodyHash() {
        return bodyHash;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public void setBodyHash(String bodyHash) {
        this.bodyHash = bodyHash;
    }
}
