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

/**
 * {

                "id":"2",
                "name2",
                "title":"标题2",
                "url":"https://www.google.com.hk/item-2",
                "publish_time":1552978902,
                "score":16.06
            },
            {
                "id":"60",
                "name60",
                "title":"标题60",
                "url":"https://www.google.com.hk/item-60",
                "publish_time":1553444982,
                "score":62.58
            },
            {
                "id":"67",
                "name67",
                "title":"标题67",
                "url":"https://www.google.com.hk/item-67",
                "publish_time":1553522957,
                "score":12.17
            },
            {
                "id":"15",
                "name15",
                "title":"标题15",
                "url":"https://www.google.com.hk/item-15",
                "publish_time":1553525421,
                "score":32.36
            },
            {
                "id":"53",
                "name53",
                "title":"标题53",
                "url":"https://www.google.com.hk/item-53",
                "publish_time":1553109227,
                "score":52.13
            },
            {
                "id":"70",
                "name70",
                "title":"标题70",
                "url":"https://www.google.com.hk/item-70",
                "publish_time":1552781921,
                "score":1.72
            },
            {
                "id":"53",
                "name53",
                "title":"标题53",
                "url":"https://www.google.com.hk/item-53",
                "publish_time":1553229003,
                "score":5.31
            },
            {
                "id":"30",
                "name30",
                "title":"标题30",
                "url":"https://www.google.com.hk/item-30",
                "publish_time":1553282629,
                "score":26.51
            },
            {
                "id":"36",
                "name36",
                "title":"标题36",
                "url":"https://www.google.com.hk/item-36",
                "publish_time":1552665833,
                "score":48.76
            }
        ]
    }
}
*/
public class Kafka2Producer {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties p = new Properties();
		p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.5.120:9092");
		p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "kafka2");
		KafkaProducer<String, String> kp = new KafkaProducer<String, String>(p);
		Result results = new Result();
		results.setId(449);
		Item[] items = new Item[10];
		items[0] = new Item(47,"name47","标题47","https://www.google.com.hk/item-47",1552884870,96.03f);
		Response response = new Response();
		response.setSearch_time(System.currentTimeMillis());
		response.setCode(200);
		response.setResults(results);
		Future<RecordMetadata> result = kp.send(new ProducerRecord<String, String>("xzq0526", JSON.toJSONString(response)));
		System.out.println(result.get().partition() + "   " + result.get());
		kp.close();
	}
}
