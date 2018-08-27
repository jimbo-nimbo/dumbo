package ir.sahab.nimbo.jimbo.main;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;
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

    @Command
    public void search() throws IOException {
        //Should have time and earliers first and related tweets link
        System.out.println("enter search text");
        String searchText = inp.nextLine();
    }

    @Command
    public void getRelatedTweets(){
        System.out.println("enter news link");
        String newsLink = inp.nextLine();

    }

    @Command
    public void getTrendWords(){

    }
    @Command
    public void getTrendNewsWithTweets(){

    }


}