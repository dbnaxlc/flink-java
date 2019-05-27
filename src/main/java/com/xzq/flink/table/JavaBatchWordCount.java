package com.xzq.flink.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class JavaBatchWordCount {
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.getTableEnvironment(env);
		String path = JavaBatchWordCount.class.getClassLoader().getResource("words.txt").getPath();
		tEnv.connect(new FileSystem().path(path))
			.withFormat(new Csv().field("word", Types.STRING).lineDelimiter("\n"))
			.withSchema(new Schema().field("word", Types.STRING))
			.registerTableSource("fileSource");
		Table result = tEnv.scan("fileSource")
			.groupBy("word")
			.select("word, count(1) as count");
		tEnv.toDataSet(result, Row.class).print();
	}
}
