package com.xzq.flink.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaJson {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Kafka kafka = new Kafka().version(KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL).topic("xzq0526")
				.startFromEarliest()
//				.startFromLatest()
				.property("bootstrap.servers", "172.16.5.120:9092,172.16.5.140:9092,172.16.5.223:9092")
				.property(ConsumerConfig.GROUP_ID_CONFIG, "xzq0526").property("session.timeout.ms", "30000");
//				.sinkPartitionerFixed();

		tableEnv.connect(kafka).withFormat(new Json().failOnMissingField(false).deriveSchema()).withSchema(new Schema()
				.field("search_time", Types.LONG).field("code", Types.INT)
				.field("results", Types.ROW_NAMED(new String[] { "id", "items" },
						new TypeInformation[] { Types.INT, ObjectArrayTypeInfo.getInfoFor(Row[].class, 
								Types.ROW_NAMED(new String[] { "id", "name", "title", "url", "publish_time", "score" },
										new TypeInformation[] { Types.INT, Types.STRING, Types.STRING, Types.STRING,
												Types.LONG, Types.FLOAT })) })))
				.inAppendMode().registerTableSource("tb_json");

		// item[1] item[10] 数组下标从1开始
		String sql4 = "select search_time, code, results.id as result_id, items[1].name as item_1_name, items[2].id as item_2_id from tb_json";

		Table table4 = tableEnv.sqlQuery(sql4);
		tableEnv.registerTable("tb_item_2", table4);
		table4.printSchema();

		TableSink sink = new CsvTableSink("E:/json/json.txt", "|");

		tableEnv.registerTableSink("console4", new String[] { "search_time", "code", "result_id", "item_1_name", "item_2_id" },
				new TypeInformation[] { Types.LONG, Types.INT, Types.INT, Types.STRING, Types.INT }, sink);

		table4.insertInto("console4");
		tableEnv.toRetractStream(table4, Row.class).print();

		// execute program
		env.execute("Flink Table Json Engine");

	}

}
