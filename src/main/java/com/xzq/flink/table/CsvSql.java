package com.xzq.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

public class CsvSql {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
		DataSet<WordCount> dataSet = env.readTextFile("G:/word.txt").map(value -> {
			String[] str = value.split(",");
			WordCount wc = new WordCount(str[0], Long.parseLong(str[1]));
			return wc;
		}).returns(WordCount.class);
		dataSet.print();
		Table table = tableEnv.fromDataSet(dataSet);
		tableEnv.registerTable("test", table);
		Table word = tableEnv.scan("test").select("word,frequency");
		word.printSchema();
		
		Table sqlQuery = tableEnv.sqlQuery("select word, sum(frequency) as total from test group by word order by total desc");
		
		DataSet<Result> result = tableEnv.toDataSet(sqlQuery, Result.class);
		result.print();
		
		TableSink sink = new CsvTableSink("G:/SQLTEST1.txt", "|");
		String[] fieldNames = {"word", "total"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
		tableEnv.registerTableSink("SQLTEST", fieldNames, fieldTypes, sink);
		sqlQuery.insertInto("SQLTEST");
		env.execute();
	}

	public static class Result {
		public String word;
		public Long total;
		public Result() {}
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Result [word=");
			builder.append(word);
			builder.append(", total=");
			builder.append(total);
			builder.append("]");
			return builder.toString();
		}
	}
}
