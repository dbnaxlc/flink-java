package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

//https://blog.csdn.net/u013411339/article/details/88143701
public class FlinkSql {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<WC> list  =  new ArrayList<WC>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            WC wc = new WC(word, 1);
            list.add(wc);
        }
        DataSet<WC> input = env.fromCollection(list);
        tableEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tableEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WC> result = tableEnv.toDataSet(table, WC.class);
        result.print();
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
