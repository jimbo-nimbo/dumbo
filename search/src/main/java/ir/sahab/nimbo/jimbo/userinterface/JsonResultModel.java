package ir.sahab.nimbo.jimbo.userinterface;

import java.util.ArrayList;

public class JsonResultModel {

    private ResultModel[] resultModels;

    public JsonResultModel(){
    }

    public JsonResultModel(int size){
        resultModels = new ResultModel[size];
    }

    public ResultModel[] getResultModels() {
        return resultModels;
    }

    public void setResultModels(ResultModel[] resultModels) {
        this.resultModels = resultModels;
    }
}
