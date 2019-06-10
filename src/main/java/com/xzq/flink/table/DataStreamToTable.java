package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.xzq.flink.dto.WordCount;

public class DataStreamToTable {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<WordCount> list  =  new ArrayList<WordCount>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            WordCount wc = new WordCount(word, 1);
            list.add(wc);
        }
		DataStreamSource<WordCount> input = env.fromCollection(list);
		Table table = tableEnv.fromDataStream(input, "word, frequency");
		System.out.println(table.tableName());
        tableEnv.toRetractStream(table, WordCount.class).print();
        env.execute();
	}

}
