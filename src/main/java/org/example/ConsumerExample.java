package org.example;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerExample {

    public static void main(final String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        final String topic = "messages";

        final Properties props = ProducerExample.loadConfig(args[0]);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(
                            String.format("Consumed message from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        }
    }
}