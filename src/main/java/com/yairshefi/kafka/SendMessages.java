package com.yairshefi.kafka;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.glassfish.jersey.SslConfigurator;

public class SendMessages {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		final String consumerKey = checkNotBlank(args[0], "consumerKey as 1st");
		final String nonce = checkNotBlank(args[1], "nonce as 2nd");
		final String signature = checkNotBlank(args[2], "signature as 3rd");
		final String token = checkNotBlank(args[3], "token as 4th");
		final String timestamp = String.valueOf(System.currentTimeMillis() / 1000);

		SslConfigurator sslConfig = SslConfigurator.newInstance()
		        .trustStoreFile("cacerts")
		        .trustStorePassword("changeit")
		        .keyStoreFile("cacerts")
		        .keyPassword("changeit");
		 
		SSLContext sslContext = sslConfig.createSSLContext();
		final Client client = ClientBuilder.newBuilder().sslContext(sslContext).build();
		
		final WebTarget target = client.target("https://api.twitter.com/1.1").path("followers/ids.json");

		final Invocation.Builder invocationBuilder =
		        target.request(MediaType.TEXT_PLAIN_TYPE);
		invocationBuilder.header("Authorization",
						"OAuth oauth_consumer_key=\"" + consumerKey + "\", " +
						"oauth_nonce=\"" + nonce + "\", " +
						"oauth_signature=\"" + signature + "\", " +
						"oauth_signature_method=\"HMAC-SHA1\", " +
						"oauth_timestamp=\"" + timestamp + "\", " +
						"oauth_token=\"" + token + "\", " +
						"oauth_version=\"1.0\"");
		
		Response response = invocationBuilder.get();
		System.out.println(response.getStatus());
		String entStr = response.readEntity(String.class);
		System.out.println(entStr);
		
		final HashMap<String, Object> configs = new HashMap<>();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
		try (final KafkaProducer<String, String> kafkaProducer =
				     new KafkaProducer<>(configs) ) {
		
			final ProducerRecord<String, String> record = new ProducerRecord<>("twitter_followers", entStr);
			final Future<RecordMetadata> sendFuture = kafkaProducer.send(record);
			
			final RecordMetadata recordMetadata = sendFuture.get();
			System.out.println("msg sent successfully: " +
							   "offset: " + recordMetadata.offset() +
							   " partition: " + recordMetadata.partition() +
							   " topic: " + recordMetadata.topic());
		}
	}

	private static String checkNotBlank(final String s, final String name) {
		if (s != null && ! s.isEmpty()) {
			return s;
		}
		throw new IllegalArgumentException(name + " argument is blank");
	}
}
