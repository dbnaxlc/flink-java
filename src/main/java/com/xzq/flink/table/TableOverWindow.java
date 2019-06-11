package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.Over;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.xzq.flink.table.dto.AccessLog;

/**
 * 
 * @author XIAZHIQIANG
 *
 */
public class TableOverWindow {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		List<Tuple3<String, Integer, Long>> list  =  new ArrayList<Tuple3<String, Integer, Long>>();
        String wordsStr = "Hello Flink Hello TOM Hello Hello";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            Tuple3<String, Integer, Long> wc = new Tuple3<String, Integer, Long>(word, 1, System.currentTimeMillis());
            list.add(wc);
            Thread.sleep(3000);
        }
        DataStream<Tuple3<String, Integer, Long>> input = env.fromCollection(list);
        DataStream<Tuple3<String, Integer, Long>> ds = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(1)) {
			private static final long serialVersionUID = 1L;

			@Override
            public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                return element.f2;
            }
        });
        tableEnv.registerDataStream("WordCount", ds, "f0,f1,f2,rowtime.rowtime");
//        unboundedRow(tableEnv);
//        unboundedRange(tableEnv);
//        boundedRange(tableEnv);
        boundedRow(tableEnv);
        env.execute("table-over");
	}
	
	/**
	 * 对于row-count使用unbounded_row来表示Unbounded
	 * @author dbnaxlc
	 * @date 2019年6月11日 下午2:53:33
	 * @param tableEnv
	 */
	public static void unboundedRow(StreamTableEnvironment tableEnv) {
		Table table = tableEnv.scan("WordCount")
        		.select("f0,f1,f2")
        		.window(Over.partitionBy("f0").orderBy("f2").preceding("unbounded_row").as("w"))
        		.select("f0, f1, f2");
        tableEnv.toRetractStream(table, Row.class).print();
	}

	/**
	 * 对于event-time及processing-time使用unbounded_range来表示Unbounded
	 * @author dbnaxlc
	 * @date 2019年6月11日 下午2:53:29
	 * @param tableEnv
	 */
	public static void unboundedRange(StreamTableEnvironment tableEnv) {
		Table table = tableEnv.scan("WordCount")
        		.select("f0,f1,f2")
        		.window(Over.partitionBy("f0").orderBy("f2").preceding("unbounded_range").as("w"))
        		.select("f0, f1, f2");
        tableEnv.toRetractStream(table, Row.class).print();
	}
	
	/**
	 * 对于row-count使用诸如10.rows来表示Bounded
	 * @author dbnaxlc
	 * @date 2019年6月11日 下午2:52:54
	 * @param tableEnv
	 */
	public static void boundedRow(StreamTableEnvironment tableEnv) {
		Table table = tableEnv.scan("WordCount")
        		.window(Over.partitionBy("f0").orderBy("rowtime").preceding("2.rows").as("w"))
        		.select("f0, f1.sum over w, f2");
        tableEnv.toRetractStream(table, Row.class).print();
	}

	/**
	 * 对于event-time及processing-time使用诸如1.minutes来表示Bounded
	 * @author dbnaxlc
	 * @date 2019年6月11日 下午2:52:58
	 * @param tableEnv
	 */
	public static void boundedRange(StreamTableEnvironment tableEnv) {
		Table table = tableEnv.scan("WordCount")
        		.select("f0,f1,f2")
        		.window(Over.partitionBy("f0").orderBy("f2").preceding("10.seconds").as("w"))
        		.select("f0, f1, f2");
        tableEnv.toRetractStream(table, Row.class).print();
	}
}
