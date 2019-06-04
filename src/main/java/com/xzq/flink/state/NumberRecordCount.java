package com.xzq.flink.state;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;


public class NumberRecordCount implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, ListCheckpointed<Integer> {

	private static final long serialVersionUID = 4004161816836172698L;
	private Integer numberRecordCount = 0;

	@Override
	public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
		return Collections.singletonList(numberRecordCount);
	}

	@Override
	public void restoreState(List<Integer> state) throws Exception {
		numberRecordCount = 0;
		numberRecordCount = state.stream().mapToInt(i -> i.intValue()).sum();
	}

	@Override
	public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
		numberRecordCount = numberRecordCount+1;
		out.collect(new Tuple2<String, Integer>(value.f0, numberRecordCount));
	}




}
