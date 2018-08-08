package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.parser.Metadata;

import java.util.List;

public class ElasticsearchWebpageModel {

    private String article;
    private String title;

    private List<Metadata> metadataList;

    public ElasticsearchWebpageModel(String article, String title, List<Metadata> metadataList){
        this.article = article;
        this.title = title;
        this.metadataList = metadataList;
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
