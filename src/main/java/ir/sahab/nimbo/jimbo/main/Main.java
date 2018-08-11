package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.crawler.Crawler;
import ir.sahab.nimbo.jimbo.elasticSearch.ElasticCannotLoadException;

import java.io.IOException;

public class Main
{
    public static void main(String[] args) throws ElasticCannotLoadException, IOException {
        Seeder.getInstance().initializeKafka();
        (new Crawler()).run();

    }
}
