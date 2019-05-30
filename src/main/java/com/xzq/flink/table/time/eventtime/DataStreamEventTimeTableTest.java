package com.xzq.flink.table.time.eventtime;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

/**
 * 添加事件时间处理列
 * 
 * @author XIAZHIQIANG
 *
 */
public class DataStreamEventTimeTableTest  {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		tableEnv.registerTableSource("table-time-attr", new DataStreamEventTimeTableSource());
		WindowedTable table = tableEnv.scan("table-time-attr").window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));
		tableEnv.toRetractStream(table.table(), Row.class).print();
		env.execute();
	}

}
