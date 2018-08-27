package ir.sahab.nimbo.jimbo;

import com.codahale.metrics.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetricsTest {
    private static final MetricRegistry metrics = new MetricRegistry();

    public static void main(String args[]) {
        List<Integer> list = new ArrayList<>();

        startJmxReport();
        final Meter requests = metrics.meter("requests");
        metrics.register(MetricRegistry.name(MetricsTest.class, "list", "size"),
                (Gauge<Integer>) list::size);
        final Counter counter = metrics.counter(MetricRegistry.name(MetricsTest.class, "counter"));
        final Timer responses = metrics.timer(MetricRegistry.name(MetricsTest.class, "responses"));


        while (true) {
            final Timer.Context context = responses.time();
            final Timer.Context context2 = responses.time();
            wait5Seconds();
            context.stop();
            context2.stop();
            list.add((int) requests.getCount());
            counter.inc();
            requests.mark();
            requests.mark();
            requests.mark();
        }
    }

    private static void startJmxReport() {
        final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
    }

    static void startConsoleReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    static void wait5Seconds() {
        try {
            Thread.sleep(5*1000);
        }
        catch(InterruptedException e) {}
    }
}
