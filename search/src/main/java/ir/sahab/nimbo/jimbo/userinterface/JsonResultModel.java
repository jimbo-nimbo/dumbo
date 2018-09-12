package ir.sahab.nimbo.jimbo.userinterface;

import java.util.ArrayList;

public class JsonResultModel {

    private ResultModel[] resultModels;
    private String[] searchKeyWord;

    public JsonResultModel(){
    }

    JsonResultModel(ResultModel[] resultModels){
        this.resultModels = resultModels;
    }

    public ResultModel[] getResultModels() {
        return resultModels;
    }

    public void setResultModels(ResultModel[] resultModels) {
        this.resultModels = resultModels;
    }

    public String[] getSearchKeyWord() {
        return searchKeyWord;
    }

    public void setSearchKeyWord(String[] searchKeyWord) {
        this.searchKeyWord = searchKeyWord;
    }
}
