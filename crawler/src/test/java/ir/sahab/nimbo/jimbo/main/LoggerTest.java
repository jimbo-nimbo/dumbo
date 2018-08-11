package ir.sahab.nimbo.jimbo.main;

import org.junit.Test;

public class LoggerTest {

    @Test
    public void getInstance() {
    }

    @Test
    public void debugLog() {
        Logger.getInstance().debugLog("test");
    }

    @Test
    public void warnLog() {
        Logger.getInstance().warnLog("test");
    }

    @Test
    public void infoLog() {
        Logger.getInstance().infoLog("test");
    }
}