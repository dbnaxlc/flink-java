 package com.xzq.flink.kafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkFromKafka {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties pro = new Properties();
		pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.5.120:9092,172.16.5.140:9092,172.16.5.223:9092");
		pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "xzq0521");
		env.addSource(new FlinkKafkaConsumer<String>("xzq0526", new SimpleStringSchema(), pro)).print();
		env.execute("flink-kafka");
	}

}
