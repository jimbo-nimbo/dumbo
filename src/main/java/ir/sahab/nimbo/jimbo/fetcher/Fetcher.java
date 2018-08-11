package ir.sahab.nimbo.jimbo.fetcher;

import ir.sahab.nimbo.jimbo.hbase.HBase;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaPropertyFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
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
    private final static Producer<Long, String> PRODUCER = new KafkaProducer<>(
            KafkaPropertyFactory.getProducerProperties());
    private final ArrayBlockingQueue queue;
    private final String topic;

    public Fetcher(ArrayBlockingQueue queue, Consumer<Long, String> consumer, String topic) {
        this.queue = queue;
        this.consumer = consumer;
        this.topic = topic;
    }

    boolean lruExist(URL url) {
        return LruCache.getInstance().exist(url.getHost());
    }

    void lruAdd(URL url) {
        LruCache.getInstance().add(url.getHost());
    }

    Document fetchUrl(URL url) throws IOException {
        return Jsoup.connect(url.toString()).validateTLSCertificates(false).get();
    }

    void consumeLink(String url) {
        try {
            URL siteUrl = new URL(url);
            Document body;
            if (!lruExist(siteUrl)) {
                if (!HBase.getInstance().existMark(url)) {
                    HBase.getInstance().putMark(url, "Marked!");
                    lruAdd(siteUrl);
                    body = fetchUrl(siteUrl);
                    if (body == null || !Validate.isValidBody(body)) {
                        return;
                    }
                    queue.put(body);
                }
            }
            else
                PRODUCER.send(new ProducerRecord<>(topic, null, url));
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
