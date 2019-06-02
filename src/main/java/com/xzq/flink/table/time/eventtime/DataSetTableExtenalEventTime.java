package com.xzq.flink.table.time.eventtime;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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

import com.xzq.flink.dto.WordCount;
import com.xzq.flink.table.dto.WordCountEventTime;

public class DataSetTableExtenalEventTime {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<Tuple2<String, String>> list  =  new ArrayList<Tuple2<String, String>>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
        	Tuple2<String, String> wc = new Tuple2<String, String>(word, "1");
            list.add(wc);
        }
		DataStreamSource<Tuple2<String, String>> input = env.fromCollection(list);
		DataStream<Tuple2<String, String>> ds = 
				input.assignTimestampsAndWatermarks(
	            new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, String>>(Time.seconds(1)) {
	                @Override
	                public long extractTimestamp(Tuple2<String, String> element) {
	                    return System.currentTimeMillis();
	                }
	            }
	        );
		
        tableEnv.registerDataStream("WordCount", input, "f0, f1");
        WindowedTable table = tableEnv.scan("WordCount").window(Tumble.over("10.minutes").on("f2").as("userActionWindow"));
		tableEnv.toRetractStream(table.table(), 
				TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})).print();
        env.execute();
	}
}
