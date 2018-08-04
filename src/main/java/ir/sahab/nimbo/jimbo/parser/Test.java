package ir.sahab.nimbo.jimbo.parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        //Document doc = Jsoup.connect("https://www.vice.com/en_uk/article/wjkzx4/in-the-uk-being-a-horse-girl-isnt-an-energy-its-a-way-of-life")
        Document doc = Jsoup.connect("http://dl2.serverdl.in/user/hadi/film/Avengers.Infinity.War.2018.720p.BluRay.x264.SalamDL_INFO.mkv")
                .get();

        Thread thread = new Thread(new PageExtractor(doc));
        thread.start();
        thread.join();

        System.out.println("Waiting for the jobs to finish...");
        Thread.sleep(5000);
    }
}
