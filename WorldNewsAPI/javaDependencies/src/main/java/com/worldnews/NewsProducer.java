import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class NewsProducer {
    public static void start() throws InterruptedException {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id", "camus");

        ProducerConfig producerConfig = new ProducerConfig(properties);

    }

    public static void main(String[] args){
        NewsProducer.start();
    }
}
