package com.xzq.flink.table.time.eventtime;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import com.xzq.flink.table.dto.WordCount;
import com.xzq.flink.table.dto.WordCountEventTime;

public class DataSetTableEventTime {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<WordCountEventTime> list  =  new ArrayList<WordCountEventTime>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
        	WordCountEventTime wc = new WordCountEventTime(word, 1, System.currentTimeMillis());
            list.add(wc);
        }
		DataStreamSource<WordCountEventTime> input = env.fromCollection(list);
		DataStream<WordCountEventTime> ds = 
				input.assignTimestampsAndWatermarks(
	            new BoundedOutOfOrdernessTimestampExtractor<WordCountEventTime>(Time.seconds(1)) {
	                @Override
	                public long extractTimestamp(WordCountEventTime element) {
	                    return element.getUserActionTime();
	                }
	            }
	        );
        tableEnv.registerDataStream("WordCount", input, "word, frequency, UserActionTime");
        WindowedTable table = tableEnv.scan("WordCount").window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));
		tableEnv.toRetractStream(table.table(), WordCountEventTime.class).print();
        env.execute();
	}
}
