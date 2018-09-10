package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.userinterface.JsonResultModel;
import ir.sahab.nimbo.jimbo.userinterface.ResultModel;

import java.util.ArrayList;

public class SearchManager {

    private static SearchManager searchManager = new SearchManager();
    private String message;
    private int t;
    private String[] results = new String[10];

    public static SearchManager getInstance(){
        return searchManager;
    }


    SearchManager() {

    }

    public JsonResultModel simpleSearch(String searchText){
        JsonResultModel jsonResultModel = new JsonResultModel();
        ResultModel[] resultModels = new ResultModel[1];
        resultModels[0] = new ResultModel("salam", " nimac ", 5);
        jsonResultModel.setResultModels(resultModels);
        return jsonResultModel;
    }
}

