package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.hbase.Hbase;
import ir.sahab.nimbo.jimbo.main.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;

public class Fetcher implements Runnable {
    private final Consumer<Long, String> consumer;
    private final Producer<Long, String> producer;
    private final ArrayBlockingQueue queue;
    private final String topic;

    Fetcher(ArrayBlockingQueue queue, Consumer<Long, String> consumer, Producer<Long, String> producer, String topic) {
        this.queue = queue;
        this.consumer = consumer;
        this.producer = producer;
        this.topic = topic;
    }

    boolean lruLookup(URL url) {
        LruCache lruCache = LruCache.getInstance();
        String host = url.getHost();
        if (!lruCache.exist(host)) {
            lruCache.add(host);
            return false;
        }
        return true;
    }

    Document fetchUrl(URL url) throws IOException {
        return Jsoup.connect(url.toString()).validateTLSCertificates(false).get();
    }

    void consumeLink(String url) {
        try {
            URL siteUrl = new URL(url);
            Document body = null;
            if (!lruLookup(siteUrl)) {
                if (!Hbase.getInstance().existMark(url)) {
                    Hbase.getInstance().putMark(url, "Marked!");
                    body = fetchUrl(siteUrl);
                    if (body == null || !Validate.isValidBody(body)) {
                        return;
                    }
                    queue.put(body);
                }
            }
            else
                producer.send(new ProducerRecord<>(topic, null, url));
        } catch (UnsupportedMimeTypeException e) {
//            Logger.getInstance().logToFile(e.getMessage());
//            System.err.println("Unsupported mime : " + url);
        } catch (MalformedURLException e) {
//            Logger.getInstance().logToFile(e.getMessage());
//            System.err.println("malformed url :  " + url);
        } catch (IOException e) {
//            Logger.getInstance().logToFile(e.getMessage());
//            System.err.println("io exception : " + url);
        } catch (InterruptedException e) {
//            System.err.println("INTERRUPTED EXCEPTION!!!");
        }
    }

    @Override
    public void run() {
        boolean running = true;
        while (running) {
            final ConsumerRecords<Long, String> consumerRecords;
            synchronized (consumer) {
                consumerRecords = consumer.poll(500);
                consumer.commitAsync();
            }
            consumerRecords.forEach(record -> consumeLink(record.value()));
        }
    }


}
