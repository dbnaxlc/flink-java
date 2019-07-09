package com.xzq.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

public class MapStateCheckPoint implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, CheckpointedFunction {

	private static final long serialVersionUID = 5102113193023297775L;
	
	private MapState<String, Integer> vs = null;
	
	/**
	 * 初始化或者恢复checkpoint时，触发
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.seconds(10))
				.setStateVisibility(StateVisibility.NeverReturnExpired)
				.setUpdateType(UpdateType.OnCreateAndWrite).build();
		MapStateDescriptor<String, Integer> msd = new MapStateDescriptor<>("map-state", Types.STRING,
				Types.INT);
		msd.enableTimeToLive(ttl);
		vs = context.getKeyedStateStore().getMapState(msd);
	}

	@Override
	public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out)
			throws Exception {
		if(vs.get(value.f0) == null) {
			vs.put(value.f0, value.f1);
		} else {
			vs.put(value.f0, vs.get(value.f0) + value.f1);
		}
		out.collect(new Tuple2<>(value.f0, vs.get(value.f0)));
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		
	}

}
