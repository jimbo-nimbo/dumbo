package ir.sahab.nimbo.jimbo.main;


import ir.sahab.nimbo.jimbo.userinterface.JsonResultModel;
import ir.sahab.nimbo.jimbo.userinterface.ResultModel;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/api")
public class EntryPoint {

    @GET
    @Path("test")
    @Produces(MediaType.TEXT_PLAIN)
    public String test() {
        return "Test";
    }

    @GET
    @Path("/{param}")
    @Produces(MediaType.APPLICATION_JSON)
    public String hello(@PathParam("param") String name) {
        return ":)";
    }

    @POST
    @Path("/ssearch")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonResultModel simpleSearch(SimpleQuerry querry) {
        return SearchManager.getInstance().simpleSearch(querry.getSearchText());
    }

}
