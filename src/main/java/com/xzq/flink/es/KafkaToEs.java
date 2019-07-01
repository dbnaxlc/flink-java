package com.xzq.flink.es;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import zipkin.DependencyLink;
import zipkin.internal.DependencyLinker;
import zipkin.internal.Util;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

public class KafkaToEs {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		Properties pro = new Properties();
		pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"172.16.5.120:9092,172.16.5.140:9092,172.16.5.223:9092");
		pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "xzq05185");
		pro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DataStream<String> ds = env.addSource(new FlinkKafkaConsumer<String>("zipkin", new SimpleStringSchema(), pro));

		Map<String, String> config = new HashMap<>();
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", "lychee-trace-es");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.16.5.30"), 9300));
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.16.5.31"), 9300));

		DataStream<Tuple2<String, Set<Span>>> spans = ds
				.flatMap((String value, Collector<Tuple2<String, Set<Span>>> rows) -> {
					JSONArray ar = JSONArray.parseArray(value);
					Set<Span> sameTraceId = new LinkedHashSet<>();
					for (int i = 0; i < ar.size(); i++) {
						SpanBytesDecoder.JSON_V2.decode(ar.get(i).toString().getBytes(Util.UTF_8), sameTraceId);
					}
					String traceId = ((JSONObject) ar.get(0)).getString("traceId");
					rows.collect(new Tuple2<String, Set<Span>>(traceId, sameTraceId));
				}).returns(new TypeHint<Tuple2<String, Set<Span>>>() {
				});
		spans.keyBy(0).reduce((value1, value2) -> {
			value1.f1.addAll(value2.f1);
			return new Tuple2<String, Set<Span>>(value1.f0, value1.f1);
		}).map(value -> {
			DependencyLinker linker = new DependencyLinker();
		    linker.putTrace(value.f1.iterator());
		    return linker.link();
		}).returns(new TypeHint<List<DependencyLink>>() {
		}).print();
		// ds.map(value -> {
		// JSONObject json = JSONObject.parseObject(value);
		// return new Tuple2<String, Integer>((String)json.get("name"), 1);
		// }).returns(new TypeHint<Tuple2<String, Integer>>() {
		// }).keyBy(0).sum(1)
		// .addSink(new ElasticsearchSink<>(config, transportAddresses,new
		// ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
		//
		// private static final long serialVersionUID = -249587173803317948L;
		//
		// @Override
		// public void process(Tuple2<String, Integer> value, RuntimeContext context,
		// RequestIndexer indexer) {
		// indexer.add(createIndexRequest(value));
		// }
		//
		// public IndexRequest createIndexRequest(Tuple2<String, Integer> element) {
		// Map<String, Object> json = new HashMap<>();
		// json.put("count", element.f1);
		// return Requests.indexRequest()
		// .index("my-index")
		// .type("my-type")
		// .id(element.f0)
		// .source(json);
		// }
		//
		//
		// } ));
		env.execute("flink-kafka");
	}

}
