package org.example;

import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;

public class ProducerExample {
    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        final Properties props = loadConfig(args[0]);
        final String topic = "messages";

        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Long numMessages = 1000L;
            for (Long i = 0L; i < numMessages; i++) {
                String msgNum = String.valueOf(i+1);
                String ts = String.valueOf(new Timestamp(System.currentTimeMillis()));

                producer.send(
                        new ProducerRecord<>(topic, msgNum, ts),
                        (event, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.printf("Produced message to topic %s: key = %-10s value = %s%n", topic, msgNum, ts);
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }

    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}