package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Session;
import org.apache.flink.table.api.java.Slide;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

/**
 * 
 * @author XIAZHIQIANG
 *
 */
public class TableWindow {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<Tuple3<String, Integer, Long>> list  =  new ArrayList<Tuple3<String, Integer, Long>>();
        String wordsStr = "Hello Flink Hello TOM Hello Hello";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            Tuple3<String, Integer, Long> wc = new Tuple3<String, Integer, Long>(word, 1, System.currentTimeMillis());
            list.add(wc);
//            Thread.sleep(3000);
        }
        DataSet<Tuple3<String, Integer, Long>> input = env.fromCollection(list);
        tableEnv.registerDataSet("WordCount", input);
        Table table = tableEnv.scan("WordCount")
        		.select("f0,f1,f2")
        		.window(Tumble.over("1.seconds").on("f2").as("w")).groupBy("w, f0").
        		select("f0, w.end as seconds, f1.avg as avgBillingAmount");
        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
//        result.print();
        
        Table table1 = tableEnv.scan("WordCount")
        		.select("f0,f1,f2")
        		.window(Slide.over("2.seconds").every("1.seconds").on("f2").as("w")).groupBy("w, f0").
        		select("f0, w.end as seconds, f1.sum as avgBillingAmount");
        DataSet<Row> result1 = tableEnv.toDataSet(table1, Row.class);
//        result1.print();
        
        Table table2 = tableEnv.scan("WordCount")
        		.select("f0,f1,f2")
        		.window(Session.withGap("2.seconds").on("f2").as("w")).groupBy("w, f0").
        		select("f0, w.end as seconds, f1.sum as avgBillingAmount");
        DataSet<Row> result2 = tableEnv.toDataSet(table2, Row.class);
        result2.print();
	}

}
