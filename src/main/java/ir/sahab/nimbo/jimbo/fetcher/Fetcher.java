package ir.sahab.nimbo.jimbo.fetcher;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Fetcher implements Runnable {
    private ArrayBlockingQueue queue;

    private Consumer<Long, String> consumer;

    /**
     *
     * @param queue
     */
    Fetcher(ArrayBlockingQueue queue, Consumer<Long, String> consumer) {
        this.queue = queue;
        this.consumer = consumer;
    }

    /**
     * @return The HTML as a document
     * @throws CloneNotSupportedException when domain already exists in LRU
     * @throws IOException when URL does not link to a valid page
     */
    Document getUrlBody(URL url) throws IOException {
        LruCache lruCache = LruCache.getInstance();
        String host = url.getHost();
        if (lruCache.exist(host)) {
            lruCache.add(host);
            return Jsoup.connect(url.toString()).get();
        }
        return null;
    }

    @Override
    public void run() {
        boolean running = true;
        while (running) {
            ConsumerRecords<Long, String> consumerRecords;
            synchronized (consumer) {
                consumerRecords = consumer.poll(5000);
            }
            consumerRecords.forEach(record -> {
                linkProcess(record.value());
            });
        }
    }

    private void linkProcess(String url){
        try {
            Document body = getUrlBody(new URL(url));
            if(body == null){
                //TODO send back to Kafka
            }
            // send (body) to parser
        } catch (UnsupportedMimeTypeException ignored) {
        } catch (IOException i) {
            //TODO log
            i.printStackTrace();
        }
    }


}
