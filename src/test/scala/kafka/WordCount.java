package kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {
    public static void count(String wordTopic, String countTopic, Properties properties) {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> lines = builder.stream(wordTopic);
        KTable<String, Long> counts = lines
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count();
        counts.to(Serdes.String(), Serdes.Long(), countTopic);

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}