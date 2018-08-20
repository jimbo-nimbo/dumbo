package ir.sahab.nimbo.jimbo.rss;

/*
 * Represents one RSS message
 */
public class RssFeedMessage {

    private String pubDate;
    private String title;
    private String description;
    private String link;
    private String author;
    private String guid;


    public String getTitle() {
        return title;
    }

    public void setPubDate(String pubDate) { this.pubDate = pubDate; }

    public String getPubDate() {
        return pubDate;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    @Override
    public String toString() {
        return "RssFeedMessage [title=" + title + ", description=" + description
                + ", link=" + link + ", author=" + author + ", guid=" + guid
                + ", pubDate=" + pubDate + "]";
    }

}