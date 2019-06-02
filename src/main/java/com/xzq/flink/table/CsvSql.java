package com.xzq.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

import com.xzq.flink.dto.WordCount;

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
		String explanation = tableEnv.explain(word);
		System.out.println(explanation);
		
		Table sqlQuery = tableEnv.sqlQuery("select word, sum(frequency) as total from test group by word order by total desc");
		DataSet<Result> result = tableEnv.toDataSet(sqlQuery, Result.class);
		result.print();
		explanation = tableEnv.explain(sqlQuery);
		System.out.println(explanation);
		
		TableSink sink = new CsvTableSink("G:/SQLTEST1.txt", "|");
		String[] fieldNames = {"word", "total"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
		tableEnv.registerTableSink("SQLTEST", fieldNames, fieldTypes, sink);
		sqlQuery.insertInto("SQLTEST");
		
		Table tableFilter = tableEnv.scan("test").filter("word = 'xzq'").groupBy("word").select("word, sum(frequency) as total, count(1) as cc");
		TableSink sink1 = new CsvTableSink("G:/SQLTEST2.txt", "|");
		String[] fieldNames1 = {"word", "total", "cc"};
		TypeInformation[] fieldTypes1 = {Types.STRING, Types.LONG, Types.LONG};
		tableEnv.registerTableSink("SQLTEST2", fieldNames1, fieldTypes1, sink1);
		tableFilter.insertInto("SQLTEST2");
		
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
