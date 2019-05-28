package com.xzq.flink.table;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

import com.xzq.flink.table.dto.AccessLog;

/**
 * 选择组窗口开始和结束时间戳
 * @author dbnxlc
 * @date 2019年5月28日 下午2:48:57
 */
public class AccessLogSql {

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
                @Override
                public long extractTimestamp(AccessLog element) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    System.out.println(" watermark -> " + format.format(getCurrentWatermark().getTimestamp()) + " eventTime -> " + format.format(element.getAccessTime()));
                    return element.getAccessTime();
                }
            }
        );
        eventTime(tableEnv, ds);
		procTime(tableEnv, ds);
		eventTimeSlid(tableEnv, ds);
		sessionTime(tableEnv, ds);
		env.execute();
	}

	public static Date localDateTime2Date(LocalDateTime localDateTime) {
		ZoneId zoneId = ZoneId.systemDefault();
		ZonedDateTime zdt = localDateTime.atZone(zoneId);
		return Date.from(zdt.toInstant());
	}
	
	public static void eventTime(StreamTableEnvironment tableEnv, DataStream<AccessLog> ds) {
		tableEnv.registerDataStream("access", ds, "region, userId,accessTime, proctime.proctime, rowtime.rowtime");
        String sql ="SELECT  region,  TUMBLE_START(rowtime, INTERVAL '2' MINUTE) AS winStart," +
        	      "  TUMBLE_END(rowtime, INTERVAL '2' MINUTE) AS winEnd, COUNT(region) AS pv " +
        	      " FROM access " +
        	      " GROUP BY TUMBLE(rowtime, INTERVAL '2' MINUTE), region";
        Table table = tableEnv.sqlQuery(sql);
        TableSink sink = new CsvTableSink("E:/access.txt", "|");
		String[] fieldNames = {"region", "winStart", "winEnd", "pv"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP,Types.LONG};
		tableEnv.registerTableSink("SQLTEST", fieldNames, fieldTypes, sink);
		table.insertInto("SQLTEST");
	}
	
	public static void procTime(StreamTableEnvironment tableEnv, DataStream<AccessLog> ds) {
		tableEnv.registerDataStream("proc_time_s", ds, "region, userId,accessTime, proctime.proctime, rowtime.rowtime");
		String sql = "select region, COUNT(region) AS pv from proc_time_s GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), region ";
		Table table = tableEnv.sqlQuery(sql);
		TableSink sink = new CsvTableSink("E:/proctime.txt", "|");
		String[] fieldNames = {"region", "pv"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
		tableEnv.registerTableSink("proc_time", fieldNames, fieldTypes, sink);
		table.insertInto("proc_time");
	}
	
	/**
	 * 滑动时间
	 * @author 夏志强
	 * @date 2019年5月28日 下午2:40:41
	 * @param tableEnv
	 * @param ds
	 */
	public static void eventTimeSlid(StreamTableEnvironment tableEnv, DataStream<AccessLog> ds) {
		tableEnv.registerDataStream("event_time_slid_s", ds, "region, userId,accessTime, proctime.proctime, rowtime.rowtime");
        String sql ="SELECT  region,  HOP_START(rowtime, INTERVAL '2' MINUTE, INTERVAL '5' MINUTE) AS winStart," +
        	      "  HOP_END(rowtime, INTERVAL '2' MINUTE, INTERVAL '5' MINUTE) AS winEnd, COUNT(region) AS pv " +
        	      " FROM event_time_slid_s " +
        	      " GROUP BY HOP(rowtime, INTERVAL '2' MINUTE, INTERVAL '5' MINUTE), region";
        Table table = tableEnv.sqlQuery(sql);
        TableSink sink = new CsvTableSink("E:/event_time_slid.txt", "|");
		String[] fieldNames = {"region", "winStart", "winEnd", "pv"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP,Types.LONG};
		tableEnv.registerTableSink("event_time_slid", fieldNames, fieldTypes, sink);
		table.insertInto("event_time_slid");
	}
	
	public static void sessionTime(StreamTableEnvironment tableEnv, DataStream<AccessLog> ds) {
		tableEnv.registerDataStream("session_time_s", ds, "region, userId,accessTime, proctime.proctime, rowtime.rowtime");
        String sql ="SELECT  region,  SESSION_START(rowtime, INTERVAL '2' MINUTE) AS winStart," +
        	      "  SESSION_ROWTIME(rowtime, INTERVAL '2' MINUTE) AS winEnd, COUNT(region) AS pv " +
        	      " FROM session_time_s " +
        	      " GROUP BY SESSION(rowtime, INTERVAL '2' MINUTE), region";
        Table table = tableEnv.sqlQuery(sql);
        TableSink sink = new CsvTableSink("E:/session_time.txt", "|");
		String[] fieldNames = {"region", "winStart", "winEnd", "pv"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP,Types.LONG};
		tableEnv.registerTableSink("session_time", fieldNames, fieldTypes, sink);
		table.insertInto("session_time");
	}
}
