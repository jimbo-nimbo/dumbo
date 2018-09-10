package ir.sahab.nimbo.jimbo.userinterface;

public class ResultModel {
    private String title;
    private String url;
    private String description;
    private int numberOfRefrences;

    public ResultModel(){

    }

    public ResultModel(String title, String url, String description, int numberOfRefrences){
        this.title = title;
        this.description = description;
        this.numberOfRefrences = numberOfRefrences;
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getNumberOfRefrences() {
        return numberOfRefrences;
    }

    public void setNumberOfRefrences(int numberOfRefrences) {
        this.numberOfRefrences = numberOfRefrences;
    }
}
