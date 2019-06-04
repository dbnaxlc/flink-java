package com.xzq.flink.state;

import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

public class CheckPointCount implements FlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>, CheckpointedFunction {

	private static final long serialVersionUID = 5102113193023297775L;
	
	private Integer operatorCount = 0;

	private ValueState<Integer> vs = null;
	
	private ListState<Integer> ls = null;
	
	/**
	 * 初始化或者恢复checkpoint时，触发
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.seconds(10))
				.setStateVisibility(StateVisibility.NeverReturnExpired)
				.setUpdateType(UpdateType.OnCreateAndWrite).build();
		ValueStateDescriptor<Integer> vsd = new ValueStateDescriptor<>("value-state", Types.INT);
		vsd.enableTimeToLive(ttl);
		vs = context.getKeyedStateStore().getState(vsd);
		
		ListStateDescriptor<Integer> lsd = new ListStateDescriptor<Integer>("list-state", Types.INT);
		ls = context.getOperatorStateStore().getListState(lsd);
		if(context.isRestored()) {
			operatorCount = StreamSupport.stream(ls.get().spliterator(), false).mapToInt(l -> l.intValue()).sum();
		}
	}

	/**
	 * 每当checkpoint时，触发
	 */
	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		ls.clear();
		ls.add(operatorCount);
	}

	@Override
	public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
		Integer temp = vs.value();
		if(temp == null) {
			temp = 0;
		}
		Integer keyedCount = temp +1;
		vs.update(keyedCount);
		operatorCount = operatorCount+1;
		out.collect(new Tuple3<String, Integer, Integer>(value.f0, keyedCount, operatorCount));
		
	}

}
