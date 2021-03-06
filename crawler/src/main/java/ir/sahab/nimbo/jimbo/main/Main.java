package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.crawler.Crawler;
import ir.sahab.nimbo.jimbo.crawler.CrawlerSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws ConnectException {
        if (args.length == 1 && args[0].equals("seeder")) {
            Seeder.getInstance().initializeKafka();
        } else if (args.length == 0) {
            new Crawler(new CrawlerSetting()).crawl();
        } else {
            System.err.println("ERROR: Bad arguments!");
            System.exit(1);
        }

    }
}
