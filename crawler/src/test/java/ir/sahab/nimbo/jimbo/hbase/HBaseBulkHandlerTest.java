package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.main.Config;
import ir.sahab.nimbo.jimbo.parser.Link;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class HBaseBulkHandlerTest {

    @Test
    public void bulkTest() throws InterruptedException {
        ArrayBlockingQueue<HBaseDataModel> queue = new ArrayBlockingQueue<>(Config.HBASE_BULK_CAPACITY);
        new Thread(new HBaseBulkHandler(queue)).start();

        for (int i = 0; i < 900; i++) {
            List<Link> links = new ArrayList<>();
            links.add(new Link(String.format("www.test%d.com", i + 1), "SomeText"));
            links.add(new Link(String.format("www.test%d.com", i + 2), "SomeText"));
            links.add(new Link(String.format("www.test%d.com", i + 3), "SomeText"));
            queue.put(new HBaseDataModel(String.format("www.test%d.com", i), links));
        }

        Thread.sleep(10000);
    }
}