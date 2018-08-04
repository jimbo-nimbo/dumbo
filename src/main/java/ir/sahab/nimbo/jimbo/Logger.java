package ir.sahab.nimbo.jimbo;


import org.slf4j.LoggerFactory;

public class Logger{
    private static org.slf4j.Logger logWriter = LoggerFactory.getLogger(Logger.class);
    private static Logger logger = null;

    private Logger(){

    }

    public static Logger getInstance(){
        if(logger == null)
            logger = new Logger();
        return logger;
    }

    public synchronized void logToFile(String s)
    {
        try
        {
            logWriter.debug(s);
        } catch (Exception e)
        {
            //
        }
    }
}
