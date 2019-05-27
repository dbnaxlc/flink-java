package com.xzq.flink.table;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

import com.xzq.flink.table.dto.AccessLog;

public class AccessLogSql {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<AccessLog> list  =  new ArrayList<AccessLog>();
		list.add(new AccessLog("ShangHai", "U0010", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0))));
		list.add(new AccessLog("BeiJing", "U1001", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0))));
		list.add(new AccessLog("BeiJing", "U2032", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 10, 0))));
		list.add(new AccessLog("BeiJing", "U1100", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 11, 0))));
		list.add(new AccessLog("ShangHai", "U0011", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 10, 0))));
		list.add(new AccessLog("ShangHai", "U0010", localDateTime2Date(LocalDateTime.of(2019, 5, 26, 12, 1, 0))));
		
		DataStreamSource<AccessLog> input = env.fromCollection(list);
        tableEnv.registerDataStream("access", input, "region, userId,accessTime");
        
        String sql =
        	      "SELECT  region,  TUMBLE_START(accessTime, INTERVAL '2' MINUTE) AS winStart," +
        	      "  TUMBLE_END(accessTime, INTERVAL '2' MINUTE) AS winEnd, COUNT(region) AS pv " +
        	      " FROM access " +
        	      " GROUP BY TUMBLE(accessTime, INTERVAL '2' MINUTE), region";

        Table table = tableEnv.sqlQuery(sql);
        TableSink sink = new CsvTableSink("E:/access.txt", "|");
		String[] fieldNames = {"region", "winStart", "winEnd", "pv"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.SQL_DATE, Types.SQL_DATE,Types.LONG};
		tableEnv.registerTableSink("SQLTEST", fieldNames, fieldTypes, sink);
		table.insertInto("SQLTEST");
		env.execute();
	}

	private static Date localDateTime2Date(LocalDateTime localDateTime) {
		ZoneId zoneId = ZoneId.systemDefault();
		ZonedDateTime zdt = localDateTime.atZone(zoneId);
		return Date.valueOf(zdt.toLocalDate());
	}
}
