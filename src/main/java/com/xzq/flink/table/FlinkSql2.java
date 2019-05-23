package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import com.xzq.flink.table.FlinkSql.WC;

//https://blog.csdn.net/u013411339/article/details/88143701
public class FlinkSql2 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<WC> list  =  new ArrayList<WC>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            WC wc = new WC(word, 1);
            list.add(wc);
        }
		DataStreamSource<WC> input = env.fromCollection(list);
        tableEnv.registerDataStream("WordCount", input, "word, frequency");
        Table table = tableEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        tableEnv.toRetractStream(table, WC.class).print();
        env.execute();
	}

	public static class WC {
        public String word;//hello
        public long frequency;//1

        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}
