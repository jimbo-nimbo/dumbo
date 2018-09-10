package ir.sahab.nimbo.jimbo.userinterface;

public class ResultModel {
    private String title;
    private String description;
    private int numberOfRefrences;

    public ResultModel(){

    }

    public ResultModel(String title, String description, int numberOfRefrences){
        this.title = title;
        this.description = description;
        this.numberOfRefrences = numberOfRefrences;
    }

    public String getTitle() {
        return title;
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

    public int getNumberOfRefrences() {
        return numberOfRefrences;
    }

    public void setNumberOfRefrences(int numberOfRefrences) {
        this.numberOfRefrences = numberOfRefrences;
    }
}
