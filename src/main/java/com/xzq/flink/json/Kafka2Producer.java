package com.xzq.flink.json;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSON;
import com.xzq.flink.json.dto.Item;
import com.xzq.flink.json.dto.Response;
import com.xzq.flink.json.dto.Result;

public class Kafka2Producer {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties p = new Properties();
		p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.5.120:9092");
		p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "kafka2");
		KafkaProducer<String, String> kp = new KafkaProducer<String, String>(p);
		Result results = new Result();
		results.setId(449);
		Item[] items = new Item[10];
		items[0] = new Item(47, "name47", "标题47", "https://www.google.com.hk/item-47", 1552884870, 96.03f);
		items[1] = new Item(2, "name2", "标题2", "https://www.google.com.hk/item-2", 1552978902, 16.06f);
		items[2] = new Item(60, "name60", "标题60", "https://www.google.com.hk/item-60", 1553444982, 62.58f);
		items[3] = new Item(67, "name67", "标题67", "https://www.google.com.hk/item-67", 1553522957, 12.17f);
		items[4] = new Item(15, "name15", "标题15", "https://www.google.com.hk/item-15", 1553525421, 32.36f);
		items[5] = new Item(53, "name53", "标题53", "https://www.google.com.hk/item-53", 1553109227, 52.13f);
		items[6] = new Item(70, "name70", "标题70", "https://www.google.com.hk/item-70", 1552781921, 1.72f);
		items[7] = new Item(53, "name53", "标题53", "https://www.google.com.hk/item-53", 1553229003, 5.31f);
		items[8] = new Item(30, "name30", "标题30", "https://www.google.com.hk/item-30", 1553282629, 26.51f);
		items[9] = new Item(36, "name36", "标题36", "https://www.google.com.hk/item-36", 1552665833, 48.76f);
		results.setItems(items);
		Response response = new Response();
		response.setSearch_time(System.currentTimeMillis());
		response.setCode(200);
		response.setResults(results);
		System.out.println(JSON.toJSONString(response));
		Future<RecordMetadata> result = kp
				.send(new ProducerRecord<String, String>("xzq0526", JSON.toJSONString(response)));
		System.out.println(result.get().partition() + "   " + result.get());
		kp.close();
	}
}
