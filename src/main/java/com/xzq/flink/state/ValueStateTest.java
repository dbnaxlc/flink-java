package com.xzq.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
				new Tuple2<String, Integer>("a", 4), new Tuple2<String, Integer>("a", 5),
				new Tuple2<String, Integer>("b", 4), new Tuple2<String, Integer>("b", 5));
		ds.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

			private static final long serialVersionUID = 1077570865848809381L;

			private ValueState<Integer> vs = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.seconds(10))
						.setStateVisibility(StateVisibility.NeverReturnExpired)
						.setUpdateType(UpdateType.OnCreateAndWrite).build();
				ValueStateDescriptor<Integer> vsd = new ValueStateDescriptor<>("value-state", Types.INT);
				vsd.enableTimeToLive(ttl);
				vs = getRuntimeContext().getState(vsd);
			}

			@Override
			public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, Integer>> out)
					throws Exception {
				Integer min = vs.value();
				if(min == null) {
					min = value.f1;
					vs.update(value.f1);
				}
				if (value.f1 < min) {
					vs.update(value.f1);
				}
				out.collect(new Tuple3<>(value.f0, value.f1, vs.value()));
			}
		});
//		.print();
		
//		ds.keyBy(0).flatMap(new NumberRecordCount()).print();
		
		ds.keyBy(0).flatMap(new CheckPointCount()).print();
		
		env.execute("value-state");
	}

}
