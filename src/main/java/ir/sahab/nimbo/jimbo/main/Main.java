package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.crawler.Crawler;

public class Main
{
    public static void main(String[] args)
    {
        Seeder.getInstance().initializeKafka();
        (new Crawler()).run();

    }
}
