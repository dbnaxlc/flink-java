package com.xzq.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapStateTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Tuple2<String, Integer>> ds = env.fromElements(new Tuple2<String, Integer>("a", 1),
				new Tuple2<String, Integer>("a", 2), new Tuple2<String, Integer>("a", 3),
				new Tuple2<String, Integer>("a", 4), new Tuple2<String, Integer>("a", 6),
				new Tuple2<String, Integer>("a", 4), new Tuple2<String, Integer>("a", 5),
				new Tuple2<String, Integer>("b", 4), new Tuple2<String, Integer>("b", 5));
		ds.keyBy(0).flatMap(new MapStateCheckPoint()).print();

		env.execute("value-state");
	}

}
