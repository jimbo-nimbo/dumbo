import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggerFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class testLogger {
    @Test
    public void log(){
        try {
            FileReader file = new FileReader("hi");
        } catch (FileNotFoundException e) {
            Logger.getLogger(getClass()).error(e.getMessage());
        }

    }
}
