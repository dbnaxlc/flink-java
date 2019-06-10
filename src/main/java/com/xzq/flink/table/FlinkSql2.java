package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.xzq.flink.dto.WordCount;

public class FlinkSql2 {

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
        tableEnv.registerDataStream("WordCount", input, "word, frequency as frequencys");
        Table table = tableEnv.sqlQuery(
                "SELECT word, SUM(frequencys) as frequency FROM WordCount GROUP BY word");
        tableEnv.toRetractStream(table, WordCount.class).filter(i -> i.f0).print();
        Table table2 = tableEnv.sqlQuery(
                "SELECT word, frequencys as frequency FROM WordCount");
        tableEnv.toAppendStream(table2, WordCount.class).print();
        env.execute();
	}

}
