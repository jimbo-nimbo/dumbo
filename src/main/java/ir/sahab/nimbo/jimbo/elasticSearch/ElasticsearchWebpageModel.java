package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.parser.Metadata;

import java.util.List;

public class ElasticsearchWebpageModel {

    private final String article;
    private final String title;
    private final String url;

    private final List<Metadata> metadataList;

    public ElasticsearchWebpageModel(String url, String article, String title, List<Metadata> metadataList){
        this.article = article;
        this.title = title;
        this.metadataList = metadataList;
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

    public List<Metadata> getMetadataList() {
        return metadataList;
    }
}
