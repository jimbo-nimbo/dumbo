package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.crawler.CrawlerSetting;
import ir.sahab.nimbo.jimbo.crawler.Crawler;

public class Main {
    public static void main(String[] args) {

        if (args.length == 1 && args[0].equals("seeder")) {
            Seeder.getInstance().initializeKafka();
        } else if (args.length == 0) {
            try {
                new Crawler(new CrawlerSetting()).crawl();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("ERROR: Bad arguments!");
        }

    }
}
