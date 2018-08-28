package ir.sahab.nimbo.jimbo.userinterface;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.ElasticClient;
import ir.sahab.nimbo.jimbo.hbase.HBase;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
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
        final int LIMIT = 30;
        int count = 0;
        int[] refs = new int[LIMIT];
        StringBuilder[] resualt = new StringBuilder[30];
        ArrayList<Integer> index = new ArrayList<>();
        for(SearchHit searchHit : searchHits){
            if(count <= LIMIT){
                index.add(count);
                Map<String, Object> map = searchHit.getSourceAsMap();
                String url = (String) map.get("url");
                refs[count] = HBase.getInstance().getNumberOfRefrences(url);
                resualt[count].append("Title: ").append(map.get("title"))
                        .append("\n").append(map.get("url")).append("\n");
                String content = map.get("content").toString();
                if(content.length() <= 500)
                    resualt[count].append(content).append("\n");
                else {
                    resualt[count].append(content.substring(0, 499)).append("...").append("\n");
                }
                resualt[count].append("-------------------------------------------------------------\n");
                count++;
            }
        }
        index.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return refs[o1] - refs[o2];
            }
        });
        for(int i = 0; i < LIMIT && i < searchHits.size(); i++) {
            System.err.println(resualt[index.get(i)]);
        }
    }

    @Command
    public void search() throws IOException {
        System.out.println("enter search text");
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
