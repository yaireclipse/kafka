package com.yairshefi.kafka;

import com.github.scribejava.apis.TwitterApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.*;
import com.github.scribejava.core.oauth.OAuth10aService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SendMessages {

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {

		final String apiKey = checkNotBlank(args[0], "apiKey as 1st");
		final String apiSecret = checkNotBlank(args[1], "apiSecret as 1st");
		final String accessTokenStr = checkNotBlank(args[2], "accessToken as 1st");
		final String accessTokenSecret = checkNotBlank(args[3], "accessTokenSecret as 2nd");

		final OAuth10aService service = new ServiceBuilder()
				.apiKey(apiKey)
				.apiSecret(apiSecret)
				.build(TwitterApi.instance());
		final Scanner in = new Scanner(System.in);

		OAuth1AccessToken accessToken = new OAuth1AccessToken(accessTokenStr, accessTokenSecret);
		final OAuthRequest request = new OAuthRequest(Verb.GET, "https://api.twitter.com/1.1/followers/ids.json");
		service.signRequest(accessToken, request);
		final Response response = service.execute(request);
		System.out.println("response: " + response.getBody());

		final HashMap<String, Object> configs = new HashMap<>();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
		try (final KafkaProducer<String, String> kafkaProducer =
				     new KafkaProducer<>(configs) ) {
		
			final ProducerRecord<String, String> record = new ProducerRecord<>("twitter_followers", response.getBody());
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
