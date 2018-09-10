package ir.sahab.nimbo.jimbo.userinterface;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.Config;
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
            ElasticClient.getInstance();
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
        final int SHOW_LIMIT = 10;
        final int SEARCH_LIMIT = Config.ES_RESULT_SIZE;
        int count = 0;
        StringBuilder[] result = new StringBuilder[SEARCH_LIMIT];
        ArrayList<Integer> index = new ArrayList<>();
        ArrayList<String> urls = new ArrayList<>();
        Long a = System.currentTimeMillis();
        for(SearchHit searchHit : searchHits){
            if(count < SEARCH_LIMIT){
                index.add(count);
                Map<String, Object> map = searchHit.getSourceAsMap();
                if(map.containsKey("url") && map.containsKey("title") && map.containsKey("content")) {
                    String url = (String) map.get("url");
                    urls.add(url);
                    result[count] = new StringBuilder();
                    result[count].append("Title: ").append(map.get("title"))
                            .append("\n").append(map.get("url")).append("\n");
                    String content = map.get("content").toString();
                    if (content.length() <= 500)
                        result[count].append(content).append("\n");
                    else {
                        result[count].append(content.substring(0, 499)).append("...\n");
                    }
                    count++;
                }
            }
        }
        System.err.println(System.currentTimeMillis() - a);
        a = System.currentTimeMillis();
        ArrayList<Integer> finalRefs = HBase.getInstance().getNumberOfReferences(urls);;
        index.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return finalRefs.get(o2) - finalRefs.get(o1);
            }
        });
        System.err.println(System.currentTimeMillis() - a);
        for(int i = 0; i < SHOW_LIMIT && i < searchHits.size(); i++) {
            System.err.println(result[index.get(i)]);
            System.err.print("#References : ");
            System.err.println(finalRefs.get(index.get(i)));
            System.err.println("-------------------------------------------------------------\n");
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
