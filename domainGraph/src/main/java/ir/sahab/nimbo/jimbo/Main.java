package ir.sahab.nimbo.jimbo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        (new AnchorFinder()).extractAnchorsToHbase();

//        List<String> hosts = new ArrayList<>();
//        for (int i = 0; i < 100; i++) {
//            for (int j = 0; j < i; j++) {
//                hosts.add(Integer.toString(i));
//            }
//        }

//        final List<Object> collect =
//        final Map<String, Long> collect = hosts
//                .stream()
//                .collect(Collectors.groupingBy(Function.identity(),
//                        Collectors.counting()));
//
//        final List<Object> collect1 = hosts
//                .stream()
//                .collect(Collectors.groupingBy(Function.identity(),
//                        Collectors.counting()))
//                .entrySet()
//                .stream().filter(stringLongEntry -> stringLongEntry.getValue() > 1)
//                .map((Function<Map.Entry<String, Long>, Object>) Map.Entry::getKey)
//                .collect(Collectors.toList());

//        final List<Object> collect1 = collect
//                .entrySet()
//                .stream()
//                .filter(stringLongEntry -> stringLongEntry.getValue() > 50)
//                .map((Function<Map.Entry<String, Long>, Object>) stringLongEntry -> stringLongEntry.getKey())
//                .collect(Collectors.toList());
//        for (Object o : collect1) {
//            System.out.println(o);
//        }
//                .forEach(stringLongEntry ->
//                        System.out.println(stringLongEntry));

    }
}
