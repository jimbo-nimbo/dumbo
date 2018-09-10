package ir.sahab.nimbo.jimbo.main;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;


@Path("/hello")
public class HelloWord{

    @GET
    @Path("/{param}")
    @Produces(MediaType.APPLICATION_JSON)
    public Greeting hello(@PathParam("param") String name) {
        return new Greeting(name);
    }

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public String helloUsingJson(Greeting greeting) {
        return greeting.getMessage() + "\n";
    }


    public class Greeting {

        private String message;

        Greeting() {

        }

        public Greeting(String name) {
            this.message = getGreeting(name);
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String name) {
            this.message = name;
        }

        private String getGreeting(String name) {
            return "Hello " + name;
        }
    }

}
