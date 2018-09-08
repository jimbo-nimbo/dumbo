package ir.sahab.nimbo.jimbo.main;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import ir.sahab.nimbo.jimbo.elasticsearch.TrendFinder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class Main {
    private static Scanner inp;
    static Thread rssThread = new Exec();

    public static void main(String[] args) {
        try {
            rssThread.start();
            inp = new Scanner(System.in);
            ShellFactory.createConsoleShell("Jimbo", "", new Main()).commandLoop();
        } catch (IOException e) {
            System.err.println("WTF??");
        }
    }

    private void printAns(List<String> ans){
        for(String s : ans){
            System.out.println(s);
            System.out.println("--------------------------------------------------");
        }
    }

    private void printAns(ArrayList<SearchHit> searchHits){
        final int LIMIT = 10;
        int count = 0;

        for(SearchHit searchHit : searchHits){
            if(count <= LIMIT){
                Map<String, Object> map = searchHit.getSourceAsMap();
                System.out.print("Title: " + map.get("title") + "\n" +
                        map.get("url") + "\n");
                String content = map.get("content").toString();
                if(content.length() <= 500)
                    System.out.println(content);
                else {
                    System.out.println(content.substring(0, 499) + "...");
                }
                System.out.println("-------------------------------------------------------------");

                count++;
            }
        }
    }

    @Command
    public void search() throws IOException {
        //Should have time and earliers first and related tweets link
        System.out.println("enter search text");
        String searchText = inp.nextLine();
        ArrayList<SearchHit> ans = ElasticClient.getInstance().simpleElasticSearch(searchText);
        printAns(ans);
    }

    @Command
    public void getRelatedTweets(){
        System.out.println("enter news link");
        String newsLink = inp.nextLine();

    }

    @Command
    public void getTrendWords(){
        List<String> trendWords = TrendFinder.findTrendWords();
        for(String word: trendWords) {
            System.out.println(word);
            System.out.println("-------------------------------------------------------------");
        }
    }
    @Command
    public void getTrendNewsWithTweets(){

    }


}