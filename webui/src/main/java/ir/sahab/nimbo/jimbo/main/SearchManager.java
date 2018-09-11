package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.userinterface.JsonResultModel;
import ir.sahab.nimbo.jimbo.userinterface.Main;
import ir.sahab.nimbo.jimbo.userinterface.ResultModel;
import ir.sahab.nimbo.jimbo.userinterface.WebHandler;

import java.util.ArrayList;

public class SearchManager {
    private static SearchManager searchManager = new SearchManager();

    public static SearchManager getInstance(){
        return searchManager;
    }


    SearchManager() {

    }

    public JsonResultModel simpleSearch(String searchText){
//        System.err.println("simpleSearch");
        return WebHandler.webSearch(searchText);
    }
}

