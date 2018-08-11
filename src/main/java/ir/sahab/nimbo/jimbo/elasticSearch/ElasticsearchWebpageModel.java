package ir.sahab.nimbo.jimbo.elasticSearch;


public class ElasticsearchWebpageModel {

    private final String article;
    private final String title;
    private final String url;
    private final String description;

    public ElasticsearchWebpageModel(String url, String article, String title, String description){
        this.article = article;
        this.title = title;
        this.description = description;
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public String getArticle() {
        return article;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }
}
