package com.xzq.flink.table.tablesource;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import com.xzq.flink.table.dto.WordCount;

public class BatchTableSourceTest {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
		tableEnv.registerTableSource("table-source", new MyTableSource());
		Table table = tableEnv.scan("table-source").filter("word = 'Hello'").groupBy("word")
				.select("word, count(1) as frequency");;
		table.printSchema();
		DataSet<WordCount> result = tableEnv.toDataSet(table, WordCount.class);
        result.print();
	}

}
