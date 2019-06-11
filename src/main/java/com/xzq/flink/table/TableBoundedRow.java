package com.xzq.flink.table;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.Over;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.xzq.flink.table.dto.AccessLog;

/**
 * 
 * @author XIAZHIQIANG
 *
 */
public class TableBoundedRow {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<AccessLog> list  =  new ArrayList<AccessLog>();
		list.add(new AccessLog("ShangHai", "U0010", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0)).getTime()));
		list.add(new AccessLog("BeiJing", "U1001", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0)).getTime()));
		list.add(new AccessLog("BeiJing", "U2032", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 10, 0)).getTime()));
		list.add(new AccessLog("BeiJing", "U1100", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 11, 0)).getTime()));
		list.add(new AccessLog("ShangHai", "U0011", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 10, 0)).getTime()));
		list.add(new AccessLog("ShangHai", "U0010", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0)).getTime()));
		
		DataStreamSource<AccessLog> input = env.fromCollection(list);
		// 设置水位线和事件时间
        DataStream<AccessLog> ds = input.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<AccessLog>(Time.seconds(1)) {
				private static final long serialVersionUID = 4174190406746300227L;

				@Override
                public long extractTimestamp(AccessLog element) {
                    return element.getAccessTime();
                }
            }
        );
        tableEnv.registerDataStream("access", ds, "region, userId,accessTime, proctime.proctime, rowtime.rowtime");
        Table table = tableEnv.scan("access")
        		.window(Over.partitionBy("region").orderBy("rowtime").preceding("1.rows").as("w"))
        		.select("region, userId.count over w, accessTime");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute("table-over");
	}
	
	public static Date localDateTime2Date(LocalDateTime localDateTime) {
		ZoneId zoneId = ZoneId.systemDefault();
		ZonedDateTime zdt = localDateTime.atZone(zoneId);
		return Date.from(zdt.toInstant());
	}
}
