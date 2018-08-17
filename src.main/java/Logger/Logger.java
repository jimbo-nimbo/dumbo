package Logger;


import org.apache.log4j.spi.LoggerFactory;

public class Logger {
    private static final org.apache.log4j.Logger logWriterRoot = org.apache.log4j.Logger.getLogger(Logger.class);
    private static final org.apache.log4j.Logger logWriterWarn = org.apache.log4j.Logger.getLogger("admin");
    private static final org.apache.log4j.Logger  logWriterInfo = org.apache.log4j.Logger.getLogger("file");
    private static final org.apache.log4j.Logger  logWriterFatal = org.apache.log4j.Logger.getLogger("fatal");
    private static final org.apache.log4j.Logger  logWriterError = org.apache.log4j.Logger.getLogger("error");
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
    public synchronized void errorLog(String s) {
        try {
            logWriterError.info(s);
        } catch (Exception e) {
            //
        }
    }
    public synchronized void fatalLog(String s) {
        try {
            logWriterFatal.info(s);
        } catch (Exception e) {
            //
        }
    }
}
