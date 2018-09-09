package ir.sahab.nimbo.jimbo.main;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Simple Jetty FileServer.
 * This is a simple example of Jetty configured as a FileServer.
 */
public class Main
{

    public static void main(String[] args) throws Exception
    {
        Server server = new Server(9129);

        ResourceHandler resource_handler = new ResourceHandler();

//        resource_handler.setDirectoriesListed(true);
//        resource_handler.setWelcomeFiles(new String[]{ "index.html" });
//        resource_handler.setResourceBase("webui/src/main/resources");
//        HandlerList handlers = new HandlerList();
//        handlers.setHandlers(new Handler[] { resource_handler, new DefaultHandler() });
//        server.setHandler(handlers);

        server.setHandler(new HelloWorld());


        server.start();
        server.join();
    }



    public static class HelloWorld extends AbstractHandler {
        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                throws IOException, ServletException {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            response.getWriter().println("<h1>Hello World</h1>");
        }
    }

}