package ir.sahab.nimbo.jimbo.main;

public class JsonNewsResultModel {
    NewsResultModel[] newsResultModels;

    public JsonNewsResultModel(){

    }

    public NewsResultModel[] getNewsResultModels() {
        return newsResultModels;
    }

    public void setNewsResultModels(NewsResultModel[] newsResultModels) {
        this.newsResultModels = newsResultModels;
    }
}
