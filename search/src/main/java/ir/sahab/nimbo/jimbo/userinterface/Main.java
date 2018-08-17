package ir.sahab.nimbo.jimbo.userinterface;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import ir.sahab.nimbo.jimbo.elastic.ElasticClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class Main {

    Scanner scanner;

    private static ElasticSearchSettings elasticSearchSettings;

    static {
        elasticSearchSettings = new ElasticSearchSettings();
    }

    public Main() {
        scanner = new Scanner(System.in);
    }

    public static void main(String[] args) {
        try {
            ShellFactory.createConsoleShell("RssProject-Jimbo", "", new Main()).commandLoop();
        } catch (IOException e) {
            System.err.println("WTF??");
        }
    }

    @Command
    private void search() throws IOException {

        System.out.println("Enter your search text:\n");

        scanner.nextLine();

        String searchText = scanner.nextLine();

        ArrayList<String> ans = ElasticClient.getInstance().simpleSearchInElasticForWebPage(searchText);

        for (String tmp : ans) {

            System.out.println(tmp);

        }
    }

    @Command
    private void advancedSearch() throws IOException {

        ArrayList<String> must = new ArrayList<>();
        ArrayList<String> mustNot = new ArrayList<>();
        ArrayList<String> should = new ArrayList<>();
        while (true) {
            System.out.println(
                    "Write \"must\" to add a phrase you absolutely want it to be in the page.\n"
                            + "write \"mustnot\" to add a phrase you don't want to see in the page.\n"
                            + "write \"should\" to add a phrase you prefer to see in the page.\n"
                            + "write \"done\" to get 10 best result.\n");
            String input = scanner.next().toLowerCase();
            scanner.nextLine();
            switch (input) {
                case "must":
                    System.out.println("Enter your phrase:\n");
                    must.add(scanner.nextLine());
                    break;
                case "mustnot":
                    System.out.println("Enter your phrase:\n");
                    mustNot.add(scanner.nextLine());
                    break;
                case "should":
                    System.out.println("Enter your phrase:\n");
                    should.add(scanner.nextLine());
                    break;
                case "done":
                    ArrayList<String> ans = ElasticClient.getInstance().advancedSearchInElasticForWebPage(must, mustNot, should);
                    for (String tmp : ans) {
                        System.out.println(tmp);
                    }
                    return;
                default:
                    System.out.println("input is not valid.\nplease try again.\n");
                    break;
            }
        }
    }

}
