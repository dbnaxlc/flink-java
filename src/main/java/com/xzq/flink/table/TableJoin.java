package com.xzq.flink.table;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.xzq.flink.table.dto.AccessLog;

/**
 * 
 * @author XIAZHIQIANG
 *
 */
public class TableJoin {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
        List<AccessLog> list  =  new ArrayList<AccessLog>();
		list.add(new AccessLog("ShangHai", "U0010", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0)).getTime()));
		list.add(new AccessLog("BeiJing", "U1001", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0)).getTime()));
		DataStreamSource<AccessLog> ds1 = env.fromCollection(list);
		Table t1 = tableEnv.fromDataStream(ds1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AccessLog>(Time.seconds(1)) {
				private static final long serialVersionUID = 4174190406746300227L;

				@Override
                public long extractTimestamp(AccessLog element) {
                    return element.getAccessTime();
                }
            }),"region, userId, accessTime, ta1.rowtime");
		list.add(new AccessLog("BeiJing", "U1100", localDateTime2Date(LocalDateTime.of(2019, 5, 27, 12, 11, 0)).getTime()));
		list.add(new AccessLog("ShangHai", "U0010", localDateTime2Date(LocalDateTime.of(2019, 5, 27, 12, 1, 0)).getTime()));
		DataStreamSource<AccessLog> ds2 = env.fromCollection(list);
		Table t2 = tableEnv.fromDataStream(ds2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AccessLog>(Time.seconds(1)) {
			private static final long serialVersionUID = 2523201671526163077L;

			@Override
            public long extractTimestamp(AccessLog element) {
                return element.getAccessTime();
            }
        }),"region as c0,userId as c1,accessTime as c2, ta2.rowtime");
//		join(tableEnv, t1, t2);
//		outerJoin(tableEnv, t1, t2);
		timeJoin(tableEnv, t1, t2);
        env.execute("table-join");
	}
	
	public static Date localDateTime2Date(LocalDateTime localDateTime) {
		ZoneId zoneId = ZoneId.systemDefault();
		ZonedDateTime zdt = localDateTime.atZone(zoneId);
		return Date.from(zdt.toInstant());
	}
	
	public static void join(StreamTableEnvironment tableEnv, Table t1, Table t2) {
		Table joinTable = t1.join(t2).where("region = c0").select("region, accessTime,userId, c1,c2");
		tableEnv.toAppendStream(joinTable, Row.class).print();
	}
	
	public static void outerJoin(StreamTableEnvironment tableEnv, Table t1, Table t2) {
		Table joinTable = t1.leftOuterJoin(t2).where("region = c0").select("region, accessTime,userId, c1,c2");
		tableEnv.toAppendStream(joinTable, Row.class).print();
	}
	
	public static void timeJoin(StreamTableEnvironment tableEnv, Table t1, Table t2) {
		Table joinTable = t1.leftOuterJoin(t2).where("region = c0 && ta1 = ta2").select("region, accessTime,userId, c1,c2");
		tableEnv.toAppendStream(joinTable, Row.class).print();
	}
}
