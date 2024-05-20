package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final long WINDOW_SIZE_MS = Duration.ofMinutes(1).toMillis();

    private static long lastClickTimestamp = 0;
    private static long clickCount = 0;

    private static double sumTemperature = 0.0;
    private static int sensorCount = 0;

    private static double totalTransactions = 0.0;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "data-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clickstreamStream = builder.stream("clickstream", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> iotSensorStream = builder.stream("iot_sensor", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> transactionsStream = builder.stream("transactions", Consumed.with(Serdes.String(), Serdes.String()));

        // Procesare pentru fluxul de date "clickstream"
        clickstreamStream.peek((key, value) -> logger.debug("Received clickstream event: {}", value))
                .mapValues(Consumer::processClickstream)
                .to("processed_clickstream");

        // Procesare pentru fluxul de date "iot_sensor"
        iotSensorStream.peek((key, value) -> logger.debug("Received IoTSensor event: {}", value))
                .mapValues(Consumer::processIoTSensor)
                .to("processed_iot_sensor");

        // Procesare pentru fluxul de date "transactions"
        transactionsStream.peek((key, value) -> logger.debug("Received Transaction event: {}", value))
                .mapValues(Consumer::processTransactions)
                .to("processed_transactions");

        // Rulează aplicația Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Adaugă shutdown hook pentru a închide corect Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // Metoda de procesare a datelor pentru fluxul "clickstream"
    private static String processClickstream(String value) {
        try {
            JsonNode jsonNode = objectMapper.readTree(value);

            long timestamp = jsonNode.get("timestamp").asLong();

            if (timestamp - lastClickTimestamp > WINDOW_SIZE_MS) {
                // S-a trecut la un nou minut; resetează numărul de clickuri
                lastClickTimestamp = timestamp;
                clickCount = 1;
            } else {
                // Încă suntem în același minut; incrementăm numărul de clickuri
                clickCount++;
            }
            logger.info("Processed Clickstream: Numarul de clickuri pe minut: {}", clickCount);
            return "Processed Clickstream: Numarul de clickuri pe minut: " + clickCount;
        } catch (Exception e) {
            logger.error("Error processing clickstream event: " + value, e);
            return "Error processing clickstream event";
        }
    }

    // Metoda de procesare a datelor pentru fluxul "iot_sensor"
    private static String processIoTSensor(String value) {

        try {
            JsonNode jsonNode = objectMapper.readTree(value);

            double temperature = jsonNode.get("temperature").asDouble();

            sumTemperature += temperature;
            sensorCount++;
            logger.info("Processed IoTSensor: Temperatura medie a senzorilor IoT in ultima ora: {}", (sumTemperature / sensorCount));
            return "Processed IoTSensor: Temperatura medie a senzorilor IoT in ultima ora: " + (sumTemperature / sensorCount);
        } catch (Exception e) {
            logger.error("Error processing IoTSensor event: " + value, e);
            return "Error processing IoTSensor event";
        }

    }

    // Metoda de procesare a datelor pentru fluxul "transactions"
    private static String processTransactions(String value) {
        try {
            JsonNode jsonNode = objectMapper.readTree(value);

            double amount = jsonNode.get("amount").asDouble();


            totalTransactions += amount;
            logger.info("Processed Transactions: Suma totala a tranzactiilor este: {}", totalTransactions);
            return "Processed Transactions: Suma totala a tranzactiilor este: " + (totalTransactions);
        } catch (Exception e) {
            logger.error("Error processing Transactions event: " + value, e);
        }
        return "Processed Transactions";
    }
}

//sabina.surdu@ubbcluj.ro
//si din fisier