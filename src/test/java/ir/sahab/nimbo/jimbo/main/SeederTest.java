package ir.sahab.nimbo.jimbo.main;

import ir.sahab.nimbo.jimbo.main.Seeder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SeederTest {

    private Seeder seeder;

    @Before
    public void setUp() throws Exception {
        seeder = Seeder.getInstance();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getInstance() {
    }

}