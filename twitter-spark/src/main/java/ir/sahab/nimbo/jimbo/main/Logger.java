package ir.sahab.nimbo.jimbo.main;

import org.slf4j.LoggerFactory;

public class Logger {
    private static org.slf4j.Logger logWriterRoot = LoggerFactory.getLogger(Logger.class);
    private static org.slf4j.Logger logWriterWarn = LoggerFactory.getLogger("admin");
    private static org.slf4j.Logger logWriterInfo = LoggerFactory.getLogger("file");
    private static Logger logger = null;


    private Logger() {
//        Properties resource = new Properties();
//        try {
//
//            InputStream inputStream = Logger.class.getResourceAsStream(LOG_PROP_DIR);
//            System.err.println(LOG_PROP_DIR);
//            System.err.println(inputStream);
//            resource.load(inputStream);
//        } catch (IOException e) {
//            System.err.println("cant run logger \n " + e.getMessage());
//        }
//        PropertyConfigurator.configure(resource);

    }

    public static Logger getInstance() {
        if (logger == null)
            logger = new Logger();
        return logger;
    }

    public synchronized void debugLog(String s) {
        try {
            logWriterRoot.debug(s);
        } catch (Exception e) {
            //
        }
    }
    public synchronized void warnLog(String s) {
        try {
            logWriterWarn.warn(s);
        } catch (Exception e) {
            //
        }
    }
    public synchronized void infoLog(String s) {
        try {
            logWriterInfo.info(s);
        } catch (Exception e) {
            //
        }
    }
}
