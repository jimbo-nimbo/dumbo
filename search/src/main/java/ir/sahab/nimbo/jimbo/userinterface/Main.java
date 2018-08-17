package ir.sahab.nimbo.jimbo.userinterface;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class Main {
    private static Scanner inp;

    public static void main(String[] args) {
        try {
            inp = new Scanner(System.in);
            ShellFactory.createConsoleShell("Jimbo", "", new Main()).commandLoop();
        } catch (IOException e) {
            System.err.println("WTF??");
        }
    }

    private void printAns(ArrayList<SearchHit> searchHits){
        final int LIMIT = 10;
        int count = 0;

        for(SearchHit searchHit : searchHits){
            if(count <= LIMIT){
                //System.out.println(searchHit.getSourceAsMap().get());
                count++;
            }
        }
    }

    @Command
    public void search() throws IOException {
        System.out.println("enter search text");
        inp.nextLine();
        String searchText = inp.nextLine();
        ArrayList<SearchHit> ans = ElasticClient.getInstance().simpleElasticSearch(searchText);
        printAns(ans);
    }

    @Command
    public void jimboSearch() throws IOException {
        ArrayList<String> must = new ArrayList<>();
        ArrayList<String> mustNot = new ArrayList<>();
        ArrayList<String> should = new ArrayList<>();
        while (true) {
            System.out.println(
                    "\"must\" : phrase absolutely in the page.\n"
                            + "\"mustnot\" phrase that don't want to see in the page.\n"
                            + "\"should\" phrases prefer to see in the page.\n"
                            + "\"done\" get result.\n");
            String input = inp.next().toLowerCase();
            inp.nextLine();
            switch (input) {
                case "must":
                    System.out.println("must phrase:\n");
                    must.add(inp.nextLine());
                    break;
                case "mustnot":
                    System.out.println("mustnot phrase:\n");
                    mustNot.add(inp.nextLine());
                    break;
                case "should":
                    System.out.println("should phrase:\n");
                    should.add(inp.nextLine());
                    break;
                case "done":
                    ArrayList<SearchHit> ans = ElasticClient.getInstance().jimboElasticSearch(must, mustNot, should);
                    printAns(ans);
                    return;
                default:
                    System.out.println("wrong keyWord");
                    break;
            }
        }
    }

}
