package ir.sahab.nimbo.jimbo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SeederTest {

  Seeder seeder;
  @Before
  public void setUp() throws Exception {
    seeder = Seeder.getInstance();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void getInstance() {}

  @Test
  public void getNextSite() {
    assertEquals(seeder.getNextSite(), "google.com");
  }
}