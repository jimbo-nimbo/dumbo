package ir.sahab.nimbo.jimbo.main;

import org.eclipse.jetty.server.AsyncNCSARequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.servlet.*;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.server.ServerProperties;

import javax.servlet.DispatcherType;
import java.util.EnumSet;

public class App {
    public static void main(String[] args) throws Exception {
        Server jettyServer = new Server(Config.PORT);
        HandlerList handlers = new HandlerList();
        jettyServer.setHandler(handlers);

        handlers.addHandler(getAccessLogHandler());
        handlers.addHandler(getMainServletContext());

        // DefaultHandler is always last for the main handler tree
        // It's responsible for Error handling of all prior handlers.
        // It will always respond (if the request reaches this far)
        handlers.addHandler(new DefaultHandler());

        try {
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jettyServer.destroy();
        }
    }
    static Handler getAccessLogHandler()
    {
        RequestLogHandler logHandler = new RequestLogHandler();
        AsyncNCSARequestLog log = new AsyncNCSARequestLog();
        log.setFilename("webui/logs/access-yyyy_mm_dd.log");
        logHandler.setRequestLog(log);
        return logHandler;
    }

    static Handler getMainServletContext()
    {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        // always need a resource base

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class,"/*");
        jerseyServlet.setInitOrder(0);

        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.classnames",
                EntryPoint.class.getCanonicalName());

        FilterHolder filterHolder = context.addFilter(CrossOriginFilter.class,"/*", EnumSet.allOf(DispatcherType.class));
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM,"*");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM,"Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM,"GET,PUT,POST,DELETE,OPTIONS");
        filterHolder.setInitParameter(CrossOriginFilter.PREFLIGHT_MAX_AGE_PARAM,"5184000");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM,"true");

        // DefaultServlet is always last for a ServletContext
        context.addServlet(DefaultServlet.class,"/");

        return context;
    }
}
