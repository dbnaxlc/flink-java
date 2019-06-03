package com.xzq.flink.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyReduceFunction {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<String, Integer>> ds = env.fromElements(new Tuple2("a", 1), new Tuple2("b", 1),
				new Tuple2("a", 2), new Tuple2("b", 2), new Tuple2("a", 3));
		ds.keyBy(0).reduce((t1, t2) -> new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1));
		
		ds.keyBy(0).reduce(new ReduceFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 9220953887174264517L;

			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
					throws Exception {
				return new Tuple2<String, Integer>("key:" + value1.f0, value1.f1+ value2.f1);
			}
		});
		
		ds.keyBy(0).reduce((value1, value2) -> new Tuple2<String, Integer>("key:" + value1.f0, value1.f1+ value2.f1));
		
		ds.keyBy(0).max(1).print().setParallelism(1);
		ds.keyBy(0).maxBy(1).print().setParallelism(1);
		
		ds.keyBy(0).countWindow(2).aggregate(new MyAvgAggregateFunction()).print();
		ds.keyBy(i -> i.f0).countWindow(2).process(new MyProcessWindowFunction()).print();
		env.execute("reduce");
	}

}
