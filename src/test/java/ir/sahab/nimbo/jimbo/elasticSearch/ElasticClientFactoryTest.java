package ir.sahab.nimbo.jimbo.elasticSearch;

import ir.sahab.nimbo.jimbo.parser.Metadata;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class ElasticClientFactoryTest {

    @Test
    public void completeTest() throws ElasticCannotLoadException, IOException, InterruptedException {
        ArrayBlockingQueue<ElasticsearchWebpageModel> queue = new ArrayBlockingQueue<>(10000);
        ElasticsearchThreadFactory elasticsearchThreadFactory = new ElasticsearchThreadFactory(queue);

        ElasticSearchThread newThread = elasticsearchThreadFactory.createNewThread();

        List<Metadata> list = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            list.add(new Metadata("name!", "property!", "content " + i + "!"));
        }

        List<ElasticsearchWebpageModel> models = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {

            Document document =
                    Jsoup.connect("https://en.wikipedia.org/wiki/" + i).validateTLSCertificates(false).get();
            models.add(new ElasticsearchWebpageModel(document.location(),
                    document.text(), document.title(), list));
            System.out.println("document number " + i);
        }

        System.out.println("action phase!! >:]");
        Thread.sleep(5000L);

        new Thread(newThread).start();

        for (ElasticsearchWebpageModel model: models){
            queue.put(model);
        }


        for (int i = 0; i < 20000; i++) {
            System.out.println("doc number : " + i + " , queue size : " + queue.size());
            queue.put(new ElasticsearchWebpageModel("This is test" + i + "!!!!",
                    "This is article!!!!", "This is title!!!!", list));
            Thread.sleep(5L);
        }
    }
}