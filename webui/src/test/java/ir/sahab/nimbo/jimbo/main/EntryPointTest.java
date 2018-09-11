package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.userinterface.JsonResultModel;
import org.junit.Test;

import static org.junit.Assert.*;

public class EntryPointTest {

    @Test
    public void simpleSearch(){
        SimpleQuerry simpleQuerry = new SimpleQuerry();
        simpleQuerry.setSearchText("trump");
        JsonResultModel jsonResultModel = SearchManager.getInstance().simpleSearch(simpleQuerry.getSearchText());
        assertNotNull(jsonResultModel);
        System.err.println(jsonResultModel.getResultModels()[0].getTitle());
    }

}