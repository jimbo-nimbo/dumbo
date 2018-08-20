package ir.sahab.nimbo.jimbo.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

class Seeder {

    private static final String SEED_NAME = "rss-seeds.csv";
    private static Seeder seeder = null;
    private final List<NewsSite> RssUrls;
    private final Scanner inp;


    private Seeder() {
        ClassLoader classLoader = getClass().getClassLoader();
        inp = new Scanner((classLoader.getResourceAsStream(SEED_NAME)));
        RssUrls = initializeUrls();
    }

    synchronized static Seeder getInstance() {
        if (seeder == null)
            seeder = new Seeder();
        return seeder;
    }

    public List<NewsSite> getUrls() {
        return RssUrls;
    }


    private List<NewsSite> initializeUrls() {
        List<NewsSite> sites = new ArrayList<>();
        while (inp.hasNext()) {
            String url = inp.next();
            String tag = inp.next();
            String attribute = inp.next();
            String value = inp.next();
            sites.add(new NewsSite(url, tag, attribute, value));
        }
        return sites;
    }

}
