package com.xzq.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class CsvTableSourceTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		CsvTableSource source = new CsvTableSource("G:/word.txt", 
				new String[] {"word", "total"}, 
				new TypeInformation[]{Types.STRING, Types.LONG});
		tableEnv.registerTableSource("csv-test", source);
		Table table = tableEnv.scan("csv-test");
		tableEnv.toRetractStream(table, Row.class).print();
//		tableEnv.toAppendStream(table, Row.class).print();
		CsvTableSink sink = new CsvTableSink("G:/word", "-");
		tableEnv.registerTableSink("word1", new String[] {"word", "total"}, 
				new TypeInformation[]{Types.STRING, Types.LONG}, sink);
		table.insertInto("word1");
		env.execute();
	}

}
