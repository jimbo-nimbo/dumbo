package ir.sahab.nimbo.jimbo.fetcher;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;

public class FetcherThreadTest {

//    @Test(expected = MalformedURLException.class)
//    public void noProtocolUrlTest() throws MalformedURLException {
//        new Fetcher("google.com");
//    }
//
//    @Test
//    public void validUrlTest() throws MalformedURLException {
//        new Fetcher("http://google.com");
//    }
//
//    @Test(expected = CloneNotSupportedException.class)
//    public void duplicateTest() throws CloneNotSupportedException, IOException {
//        try{
//            new Fetcher("https://sahab.ir/").getUrlBody();
//        } catch(CloneNotSupportedException c) {
//            Assert.fail();
//        }
//        new Fetcher("https://sahab.ir/contact-us/").getUrlBody();
//    }
//
//    @Test(expected = IOException.class)
//    public void invalidUrlTest() throws CloneNotSupportedException, IOException {
//        new Fetcher("https://www.programcreek.com/1997").getUrlBody();
//    }
//
//    @Test
//    public void fetchBodyTest() throws CloneNotSupportedException, IOException {
//        String content = "<html>\n" +
//                " <head> \n" +
//                "  <title>Wired Women's Studies: Courses</title> \n" +
//                "  <link REL=\"STYLESHEET\" TYPE=\"text/css\" HREF=\"wws.css\"> \n" +
//                " </head> \n" +
//                " <body bgcolor=\"FF9900\"> \n" +
//                "  <table border=\"0\" align=\"CENTER\" width=\"100%\"> \n" +
//                "   <tbody>\n" +
//                "    <tr align=\"CENTER\"> \n" +
//                "     <td height=\"45%\" align=\"center\"> <p> ___<a href=\"elearning.html\">e-learning</a>___research projects___<a href=\"modules.html\">modules</a>___<a href=\"swp1.html\">diy web pages</a>___<a href=\"mailto:eakn1@york.ac.uk\">contact</a>___</p> <p><a href=\"col3.html\"><img SRC=\"embossline.gif\" ALT=\"return to index\" BORDER=\"0\"></a> </p> <p> &nbsp; </p> </td> \n" +
//                "    </tr> \n" +
//                "   </tbody>\n" +
//                "  </table>   \n" +
//                " </body>\n" +
//                "</html>";
//        Assert.assertEquals(new Fetcher("https://www.york.ac.uk/teaching/cws/wws/projects.html").getUrlBody().toString(), content);
//    }
}