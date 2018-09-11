package ir.sahab.nimbo.jimbo.main;

public class JsonResultModel {
    NewsResultModel[] newsResultModels;

    public JsonResultModel(){

    }

    public NewsResultModel[] getNewsResultModels() {
        return newsResultModels;
    }

    public void setNewsResultModels(NewsResultModel[] newsResultModels) {
        this.newsResultModels = newsResultModels;
    }
}
