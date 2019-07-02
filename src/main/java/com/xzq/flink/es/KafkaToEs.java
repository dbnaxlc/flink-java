package com.xzq.flink.es;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

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
		pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "xzq0525");
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
		}).addSink(new ElasticsearchSink<>(config, transportAddresses,
				new ElasticsearchSinkFunction<List<DependencyLink>>() {

					private static final long serialVersionUID = -249587173803317948L;

					public IndexRequest createIndexRequest(DependencyLink element) {
						Map<String, Object> json = new LinkedHashMap<>();
						json.put("id", element.parent + "|" + element.child);
						json.put("parent", element.parent);
						json.put("child", element.child);
						json.put("callCount", element.callCount);
						json.put("errorCount", element.errorCount);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						return Requests.indexRequest().index("zipkin:dependency-" + sdf.format(new Date()))
								.type("dependency").id(element.parent + "|" + element.child).source(json);
					}

					@Override
					public void process(List<DependencyLink> value, RuntimeContext context, RequestIndexer indexer) {
						for (DependencyLink link : value) {
							indexer.add(createIndexRequest(link));
						}
					}

				}));
		env.execute("flink-zipkin");
	}

}
