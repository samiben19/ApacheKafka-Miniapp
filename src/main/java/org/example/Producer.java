package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static final Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static void createTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(Arrays.asList(
                    new NewTopic("clickstream", 1, (short) 1),
                    new NewTopic("iot_sensor", 1, (short) 1),
                    new NewTopic("transactions", 1, (short) 1)
            )).all().get();
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        createTopics();
        KafkaProducer<String, String> producer = createProducer();

        new Thread(() -> produceClickstream(producer)).start();
        new Thread(() -> produceIoTSensor(producer)).start();
        new Thread(() -> produceTransactions(producer)).start();
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void produceClickstream(KafkaProducer<String, String> producer) {

        while (true) {
            try {
                String page = random.nextBoolean() ? "home" : "product";
                String event = objectMapper.writeValueAsString(new ClickstreamEvent(page, System.currentTimeMillis()));
                producer.send(new ProducerRecord<>("clickstream", event));
                logger.debug("Sent clickstream event: {}", event);
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void produceIoTSensor(KafkaProducer<String, String> producer) {

        while (true) {
            try {
                String event = objectMapper.writeValueAsString(new IoTSensorEvent(random.nextInt(10), random.nextDouble() * 10 + 20, System.currentTimeMillis()));
                producer.send(new ProducerRecord<>("iot_sensor", event));
                logger.debug("Sent IoTSensor event: {}", event);
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void produceTransactions(KafkaProducer<String, String> producer) {

        while (true) {
            try {
                String event = objectMapper.writeValueAsString(new TransactionEvent(random.nextInt(10000), random.nextDouble() * 1000, System.currentTimeMillis()));
                producer.send(new ProducerRecord<>("transactions", event));
                logger.debug("Sent Transaction event: {}", event);
                TimeUnit.SECONDS.sleep(3);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    static class ClickstreamEvent {
        public String page;
        public long timestamp;

        public ClickstreamEvent(String page, long timestamp) {
            this.page = page;
            this.timestamp = timestamp;
        }
    }

    static class IoTSensorEvent {
        public int sensorId;
        public double temperature;
        public long timestamp;

        public IoTSensorEvent(int sensorId, double temperature, long timestamp) {
            this.sensorId = sensorId;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }
    }

    static class TransactionEvent {
        public int transactionId;
        public double amount;
        public long timestamp;

        public TransactionEvent(int transactionId, double amount, long timestamp) {
            this.transactionId = transactionId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }
}
