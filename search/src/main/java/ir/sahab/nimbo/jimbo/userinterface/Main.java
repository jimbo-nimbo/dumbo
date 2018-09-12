package ir.sahab.nimbo.jimbo.userinterface;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.ElasticClientSearch;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import static ir.sahab.nimbo.jimbo.userinterface.WebHandler.getAns;


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


    public JsonResultModel getJsonAns(){

        return new JsonResultModel();
    }

    private void printAns(ArrayList<SearchHit> searchHits){
        JsonResultModel jsonResultModel = getAns(searchHits);
//        for (int i = 0; i < jsonResultModel.getSearchKeyWord().length; i++)
//            System.out.print(jsonResultModel.getSearchKeyWord()[i]);
        System.out.println();
        for(int i = 0; i < jsonResultModel.getResultModels().length; i++) {
            System.out.println("title : " + jsonResultModel.getResultModels()[i].getTitle());
            System.out.println(jsonResultModel.getResultModels()[i].getUrl());
            System.out.println(jsonResultModel.getResultModels()[i].getDescription());
            System.out.println("#References : " + jsonResultModel.getResultModels()[i].getNumberOfRefrences());
            for(int j = 0; j < 3 && j < jsonResultModel.getResultModels()[j].getKeyWords().length; j++){
                System.out.println("keyword : " + jsonResultModel.getResultModels()[j].getKeyWords()[j]);
            }
            System.out.println("#References : " + jsonResultModel.getResultModels()[i].getNumberOfRefrences());
            System.out.println("-------------------------------------------------------------\n");
        }

    }




    @Command
    public void search() throws IOException {
        System.out.println("enter search text");
        String searchText = inp.nextLine();
        ArrayList<SearchHit> ans = ElasticClientSearch.getInstance().simpleElasticSearch(searchText);
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
                    ArrayList<SearchHit> ans = ElasticClientSearch.getInstance().jimboElasticSearch(must, mustNot, should);
                    printAns(ans);
                    return;
                default:
                    System.out.println("wrong keyWord");
                    break;
            }
        }
    }

}
