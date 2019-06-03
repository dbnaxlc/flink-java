package com.xzq.flink.datastream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyAvgAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double> {

	/**
	 * 定义累加器
	 */
	@Override
	public Tuple2<Integer, Integer> createAccumulator() {
		return new Tuple2<Integer, Integer>(0,0);
	}

	/**
	 * 定义数据添加逻辑
	 */
	@Override
	public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
		return new Tuple2(accumulator.f0 + 1, accumulator.f1 + value.f1);
	}

	/**
	 * 根据accumulator获得结果
	 */
	@Override
	public Double getResult(Tuple2<Integer, Integer> accumulator) {
		return accumulator.f1 *1.0 / accumulator.f0;
	}

	/**
	 * 合并accumulator
	 */
	@Override
	public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
		return new Tuple2(a.f0+b.f0, a.f1 + b.f1);
	}

}
