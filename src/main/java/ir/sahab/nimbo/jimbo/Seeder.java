package ir.sahab.nimbo.jimbo;

import java.util.Scanner;

public class Seeder {

    private static final String SEED_NAME = "top-200.csv";
    private static Seeder seeder = null;
    private Scanner inp;
    private Seeder() {
        ClassLoader classLoader = getClass().getClassLoader();
        inp = new Scanner((classLoader.getResourceAsStream(SEED_NAME)));
    }

    synchronized public static Seeder getInstance(){
        if(seeder == null)
            seeder = new Seeder();
        return seeder;
    }

    public String getNextSite(){
        return inp.next();
    }

}
