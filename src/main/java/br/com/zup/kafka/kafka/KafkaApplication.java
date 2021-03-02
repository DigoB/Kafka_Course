package br.com.zup.kafka.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var producer = new KafkaProducer<String, String>(properties());
		var value = "1,1,1234";
		var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);
		Callback callBack = (data,ex) -> {
			if(ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("Sucesso, enviando: " + data.topic() + " partition " + data.partition() + " / offset " + data.offset() 
			+ " / timestamp " + data.timestamp());
		};
		var email = "Thank you for your order! It is already been processed!";
		var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", email, email);
		producer.send(record, callBack).get();
		producer.send(emailRecord, callBack).get();
	}

	private static Properties properties() {

		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

		// Serializa de String para byte
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
