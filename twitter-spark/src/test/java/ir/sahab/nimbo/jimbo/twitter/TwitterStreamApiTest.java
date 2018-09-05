package ir.sahab.nimbo.jimbo.twitter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class TwitterStreamApiTest {

    TwitterStreamApi twitterStreamApi;
    @Before
    public void setUp() throws Exception {
        twitterStreamApi = new TwitterStreamApi();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void streamCountTest() throws InterruptedException {
        Thread.sleep(10000);
        assertNotEquals(0L, twitterStreamApi.getCounter());
    }
    //
    //
    // @Test
    public void getPerSecondNotTest(){
        while (true) {
            twitterStreamApi.getSpeedPerSecond();
        }
    }
}