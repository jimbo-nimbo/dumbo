package ir.sahab.nimbo.jimbo.fetcher;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import ir.sahab.nimbo.jimbo.kafaconfig.KafkaTopics;
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
    private ArrayBlockingQueue queue;

    private final Consumer<Long, String> consumer;
    private final Producer<Long, String> producer;

    Fetcher(ArrayBlockingQueue queue, Consumer<Long, String> consumer, Producer<Long, String> producer) {
        this.queue = queue;
        this.consumer = consumer;
        this.producer = producer;
    }

    /**
     * @return The HTML as a document
     * @throws IOException when URL does not link to a valid page
     */
    Document getUrlBody(URL url) throws IOException {
        LruCache lruCache = LruCache.getInstance();
        String host = url.getHost();
        if (!lruCache.exist(host)) {
            lruCache.add(host);
            return Jsoup.connect(url.toString()).validateTLSCertificates(false).get();
        }
        return null;
    }

    private void linkProcess(String url){
        try {
            Document body = getUrlBody(new URL(url));
            if(body == null){
                producer.send(new ProducerRecord<Long, String >(KafkaTopics.URL_FRONTIER.toString(),
                        null, url));
            } else {
                queue.add(body);
            }
        } catch (UnsupportedMimeTypeException ignored) {
            System.err.println(" unsupported exception: " + url);

        } catch (MalformedURLException e)
        {
            System.err.println( " type 2 : " + url);
        } catch (IOException e)
        {
        }
    }

    @Override
    public void run() {
        boolean running = true;
        while (running) {
            ConsumerRecords<Long, String> consumerRecords;
            synchronized (consumer) {
                consumerRecords = consumer.poll(500);
                consumer.commitAsync();
            }
            consumerRecords.forEach(record -> linkProcess(record.value()));
        }
    }


}
