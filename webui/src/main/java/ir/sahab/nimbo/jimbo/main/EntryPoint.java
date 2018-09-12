package ir.sahab.nimbo.jimbo.main;


import ir.sahab.nimbo.jimbo.userinterface.JsonResultModel;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/api")
public class EntryPoint {

    @GET
    @Path("/test")
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
    public JsonResultModel simpleSearch(SimpleQuery query) {
//        System.err.println("thi");
        return SearchManager.getInstance().simpleSearch(query.getSearchText());
    }

}
