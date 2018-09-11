package ir.sahab.nimbo.jimbo.main;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Main {
    private static Scanner inp;
    private static Thread rssThread = new Exec();

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
        JsonResultModel jsonResultModel = WebHandler.getAns(searchHits);
        for(int i = 0; i < jsonResultModel.getNewsResultModels().length; i++) {
            System.out.println("title : " + jsonResultModel.getNewsResultModels()[i].getTitle());
            System.out.println(jsonResultModel.getNewsResultModels()[i].getUrl());
            System.out.println(jsonResultModel.getNewsResultModels()[i].getContent());
            System.out.println("-------------------------------------------------------------\n");
        }

    }

    private void printAns2(ArrayList<SearchHit> searchHits){

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
    public void getTrendWords(){
        //TODO complete
//        List<String> trendWords = TrendFinder.getInstance().findTrendWords();
//        for(String word: trendWords) {
//            System.out.println(word);
//            System.out.println("------------");
//        }
    }

    @Command
    public void getTrendNewsWithTweets(){
//        ArrayList<String> trends = ElasticsearchHandler..findTrendWords();
//        ArrayList<SearchHit> ans = ElasticClient.getInstance().jimboElasticSearch(new ArrayList<>(), new ArrayList<>(), trends);
//        printAns(ans);
    }


}