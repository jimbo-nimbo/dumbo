package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.userinterface.JsonResultModel;
import org.junit.Test;

import static org.junit.Assert.*;

public class EntryPointTest {

    @Test
    public void simpleSearch(){
        SimpleQuery simpleQuery = new SimpleQuery();
        simpleQuery.setSearchText("hay in art");
        JsonResultModel jsonResultModel = SearchManager.getInstance().simpleSearch(simpleQuery.getSearchText());
        assertNotNull(jsonResultModel.getResultModels()[0].getKeyWords()[0]);
    }
}