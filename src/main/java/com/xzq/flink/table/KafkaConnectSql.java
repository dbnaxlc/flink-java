package com.xzq.flink.table;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * StreamTableEnvironment的connect方法创建StreamTableDescriptor；
 * StreamTableDescriptor继承了ConnectTableDescriptor；
 * ConnectTableDescriptor提供了withSchema方法，返回Schema
 * Schem提供了field、from、proctime、rowtime方法用于定义Schema的相关属性；
 * 通过proctime定义processing-time，通过rowtime定义event-time，通过from定义引用或别名
 * Rowtime提供了timestampsFromField、timestampsFromSource、timestampsFromExtractor方法用于定义timestamps；
 * 提供了watermarksPeriodicAscending、watermarksPeriodicBounded、watermarksFromSource、watermarksFromStrategy方法用于定义watermark strategies
 * @author dbnaxlc
 * @date 2019年5月30日 上午11:57:46
 */
public class KafkaConnectSql {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		Properties pro = new Properties();
		pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"172.16.5.120:9092,172.16.5.140:9092,172.16.5.223:9092");
		pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "xzq0521");
		tableEnv.connect(
				new Kafka().version(KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL).topic("xzq0526").startFromGroupOffsets()
						.property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
								"172.16.5.120:9092,172.16.5.140:9092,172.16.5.223:9092")
						.property(ConsumerConfig.GROUP_ID_CONFIG, "xzq0526"))
				.withFormat(new Json().failOnMissingField(true).deriveSchema())
				.withSchema(new Schema().field("name", Types.STRING).field("msg", Types.STRING).field("requestId", Types.STRING))
				.inAppendMode()
				.registerTableSource("access_log");
		String sql="select * from access_log";
        Table result = tableEnv.sqlQuery(sql);
        result.printSchema();
        tableEnv.toAppendStream(result, Row.class).print();
        
        String sql1= "select name,count(*) as total  from access_log group by name";
        Table total = tableEnv.sqlQuery(sql1);
        total.printSchema();
        //true表示添加消息，false标志表示撤消消息
        //在RetractModel模式中，比如原始记录为(FT,B,2)， 如果增加(FT,B,1)一条记录，它会先把(FT,B,2)删除，然后新增(FT,B,2+1)这条记录
        tableEnv.toRetractStream(total, Row.class).print();
        env.execute();
	}

}
