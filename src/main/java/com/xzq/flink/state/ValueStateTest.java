package com.xzq.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ValueStateTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Tuple2<String, Integer>> ds = env.fromElements(new Tuple2<String, Integer>("a", 1),
				new Tuple2<String, Integer>("a", 2), new Tuple2<String, Integer>("a", 3),
				new Tuple2<String, Integer>("a", 4), new Tuple2<String, Integer>("a", 6),
				new Tuple2<String, Integer>("a", 4), new Tuple2<String, Integer>("a", 5));
		ds.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

			private static final long serialVersionUID = 1077570865848809381L;

			private ValueState<Integer> vs = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				ValueStateDescriptor<Integer> vsd = new ValueStateDescriptor<>("value-state", Types.INT);
				vs = getRuntimeContext().getState(vsd);
			}

			@Override
			public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
				Integer min = vs.value();
				if(min == null) {
					min = 100;
				}
				if (value.f1 < min) {
					vs.update(value.f1);
				} 
				out.collect(new Tuple3<>(value.f0, value.f1, vs.value()));
			}
		}).print();
		env.execute("value-state");
	}

}
