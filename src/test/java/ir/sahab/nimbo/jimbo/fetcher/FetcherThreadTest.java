package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.net.MalformedURLException;

import static org.junit.Assert.*;

public class FetcherThreadTest {

    @Test(expected = MalformedURLException.class)
    public void noProtocolUrlTest() throws MalformedURLException {
        new FetcherThread("google.com");
    }

    @Test
    public void validUrlTest() throws MalformedURLException {
        new FetcherThread("http://google.com");
    }

    @Test(expected = CloneNotSupportedException.class)
    public void duplicateTest() throws CloneNotSupportedException, IOException {
        try{
            new FetcherThread("https://sahab.ir/").getUrlBody();
        } catch(CloneNotSupportedException c) {
            Assert.fail();
        }
        new FetcherThread("https://sahab.ir/contact-us/").getUrlBody();
    }

    @Test(expected = IOException.class)
    public void invalidUrlTest() throws CloneNotSupportedException, IOException {
        new FetcherThread("https://sahab.ir/contact-you").getUrlBody();
    }
}